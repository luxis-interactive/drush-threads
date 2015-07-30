<?php

require_once 'includes/Import.inc';

date_default_timezone_set('America/Costa_Rica');

/**
 * Initialize commands
 **/
function camp_drush_command()
{
  $items = array();

  $items['import-cars'] = array(
    'description' => 'Import cars from API and save on database.',
    'options' => array(
      'threads' => 'The number of threads to use',
      'limit' => 'The total number of jobs to put on the queue',
      'batch_size' => 'How many items from the queue to process in each thread. Default is 10.',
    ),
  );

  $items['cars-consumer'] = array(
    'description' => 'Execute queue',
    'arguments' => array(
      'limit' => 'The number of jobs to process on the queue',
    )
  );

  return $items;
}

/**
 * Importing cars and append items to the queue. Initializing threads.
 *
 **/
function drush_camp_import_cars()
{
  $time       = time();
  $threads    = drush_get_option('threads', 1);
  $limit      = drush_get_option('limit', 100);
  $batch_size = drush_get_option('batch_size', 100);

  Import::addItemsToQueue();

  $queue = DrupalQueue::get('import_cars', TRUE);
  $queue_size = $queue->numberOfItems();

  try {

    drush_print("Going to work on {$queue_size} items from the queue with {$threads} threads...");

    _thread_manager($queue_size, $batch_size, $threads, '_cars_setup');
  } catch (Exception $e) {

    drush_set_error($e->getMessage());
  }

  $time = time() - $time;
  drush_print("Time taken with {$threads} threads: {$time} seconds");
}

/**
 * A test multi threaded setup function.
 *
 * @param int $thread_id
 *   An identifier for the thread which will execute this command.
 * @param int $batch_size
 *   How many tasks this command should work on.
 * @param int $offset
 *   The position in a queue of jobs for the first job this command
 *   should work on.
 *
 * @return string
 *   A command which can be executed on the command line.
 */
function _cars_setup($thread_id, $batch_size, $offset) {
  return "drush cars-consumer $batch_size";
}

/**
 * Consume items from the queue.
 *
 * @param int $limit
 *   The maximum number of items to consume.
 */
function drush_camp_cars_consumer($limit = 100)
{
  drush_print(' -> Starting consumers');

  Import::processItemsOnQueue();
}

/**
 * A multithreading manager for drush.
 *
 * @param $job_count
 *  The total number of jobs to process
 *
 * @param $job_batch_size
 *  The number of jobs handed to each thread
 *
 * @param $thread_count
 *  The maximum number of active threads
 *
 * @param $setup_function
 *  The function to generate the thread command. This is written for your
 *  application its signature is:
 *  my_setup_function($thread_id, $limit, $offset) : string;
 *  Where $thread_id is the unique thread identifier, $limit is the total
 *  number of records to process and offset is the starting point. The
 *  function should return a string which is a shell command that the thread
 *  will execute in a separate process.
 *
 * @param string $teardown_function
 *  A function which runs when a thread completes. It's signature is:
 *  my_teardown_function($thread_id);
 *  Your business logic can then run any cleanup which occurs when the
 *  thread completes.
 *
 * @param int $starting_offset
 *  (optional) Start x records into the process.
 *
 * @throws Exception
 */
function _thread_manager($job_count, $job_batch_size, $thread_count, $setup_function, $teardown_function = '', $starting_offset = 0)
{

  // Function of PHP for threading
  if (!function_exists('proc_open')) {
    throw new Exception('proc_open command is needed to use multi threading.');
  }

  $jobs_remaining = $job_count;
  $offset         = $starting_offset;
  $running        = TRUE;
  $processes      = array();
  $start          = time();

  $descriptor_spec = array(
    0 => array("pipe", "r"),
    1 => array("pipe", "w"),
    2 => array("pipe", "w"),
  );

  while ($running) {

    // Fill all empty threads.
    for ($thread_id = 0; $thread_id < $thread_count; $thread_id++)
    {
      if ($jobs_remaining > 0 && ! isset($processes[$thread_id]))
      {
        $pipes    = array();
        $cmd      = $setup_function($thread_id, $job_batch_size, $offset);
        $process  = proc_open($cmd, $descriptor_spec, $pipes);

        drush_print("Thread {$thread_id} starts at offset $offset. Jobs remaining: $jobs_remaining");

        foreach ($pipes as $pipe) {
          stream_set_blocking($pipe, FALSE);
        }

        $offset += $job_batch_size;
        $jobs_remaining -= $job_batch_size;

        $processes[$thread_id] = array(
          'process' => $process,
          'pipes'   => $pipes,
        );

        //sleep(1);
      }
    }

    // Monitor running threads.
    for ($thread_id = 0; $thread_id < $thread_count; $thread_id++)
    {
      if ( ! isset($processes[$thread_id]))
      {
        continue;
      }

      $process_running = _monitor_process($thread_id, $processes[$thread_id]['process'], $processes[$thread_id]['pipes']);

      if ( ! $process_running)
      {
        unset($processes[$thread_id]);

        if ( ! empty($teardown_function))
        {
          $teardown_function($thread_id);
        }

        $speed    = ($job_count - $jobs_remaining) / (time() - $start);
        $end_date = date('G:i:s d/m/y', ($job_count / $speed) + $start);
        $speed    = floor($speed * 60);

        drush_print("Thread {$thread_id} finished. Average speed is {$speed} jobs/minute. Estimated completion at {$end_date}");
      }
    }

    // Check for endgame.
    if ($jobs_remaining < 1 && empty($processes))
    {
      $running = FALSE;
    }
    else
    {
      // Pause the master process.
      sleep(1);
    }
  }
}

/**
 * Monitor a process.
 *
 * This function checks a running process.
 * If it has finished it is closed down safely.
 *
 * @param $thread_id
 * @param $process
 * @param $pipes
 * @return bool
 *  TRUE if the process is still running
 *  FALSE if it is not running.
 */
function _monitor_process($thread_id, $process, $pipes)
{
  // Get the status of proc_open()
  $status = proc_get_status($process);

  foreach ($pipes as $id => $pipe)
  {
    if ($id == 0)
    {
      // Don't read from stdin!
      continue;
    }

    $messages = stream_get_contents($pipe);

    if ( ! empty($messages))
    {
      $messages = explode("\n", $messages);

      foreach ($messages as $message)
      {
        $message = str_replace('[error]', '', $message);
        $message = str_replace('[completed]', '', $message);
        $message = trim($message);

        if ( ! empty($message))
        {
          if ($id == 1)
          {
            // Message from stdout.
            drush_print(" -> T$thread_id: $message");
          }
          else
          {
            // Message from stderr.
            drush_set_error(" -> T$thread_id: $message");
          }
        }
      }
    }
  }

  if ( ! $status['running']) {
    foreach ($pipes as $pipe) {
      fclose($pipe);
    }

    proc_close($process);
  }

  return (bool)$status['running'];
}
