<?php

require_once 'Api.inc';

final class Import extends Api
{

  /**
   * Get cars from API
   *
   * @return Object
   **/
  private static function getCarsFromApi()
  {
    $url = 'http://dev-prueba-luxis.pantheon.io/cars';

    $response = self::call($url);

    if( $response->code != 200)
    {
      throw new Exception("API unresponsive: {$response->error}");
    }

    return json_decode($response->data);
  }

  /**
   * Add item on queue
   *
   * @throws Exception
   **/
  public static function addItemsToQueue()
  {
    $queue = DrupalQueue::get('import_cars');

    try {
      $cars = self::getCarsFromApi();

      foreach($cars->cars as $car)
      {
        $queue->createItem($car->car);
      }

    } catch(Exception $e) {

      drush_set_error($e->getMessage());
    }
  }

  /**
   * Process all items on queue
   **/
  public static function processItemsOnQueue()
  {
    $queue = DrupalQueue::get('import_cars');

    while( $car = $queue->claimItem() )
    {
      $transaction = db_transaction(__FUNCTION__);

      try {
        self::saveCarOnDrupal($car->data);
        $queue->deleteItem($car);

      } catch( Exception $e) {

        $transaction->rollback();
        $queue->deleteItem($car);
        drush_set_error($e->getMessage());
        return;
      }

      unset($transaction);
    }
  }

  /**
   * Create new car on drupal
   *
   * @return bool
   **/
  private static function saveCarOnDrupal($car)
  {
    $node = new stdClass();

    $node->type = 'cars';

    // Initialize node for drupal
    node_object_prepare($node);

    // Published
    $node->status = 1;
    $node->title = $car->title;

    $node->language = LANGUAGE_NONE;

    $node->field_model[$node->language][0]['value']     = $car->model;
    $node->field_photo_url[$node->language][0]['value'] = $car->photo_url->src;
    $node->field_price[$node->language][0]['value']     = $car->price;

    node_save($node);

    drush_print("New car is saved with id: {$node->nid}");

    return ! empty($node->nid);
  }
}
