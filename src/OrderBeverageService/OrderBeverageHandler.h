#ifndef VENDING_MACHINE_MICROSERVICES_ORDERBEVERAGEHANDLER_H
#define VENDING_MACHINE_MICROSERVICES_ORDERBEVERAGEHANDLER_H

#include <iostream>
#include <string>
#include <regex>
#include <future>

#include "../../gen-cpp/OrderBeverageService.h"
#include "../../gen-cpp/WeatherService.h"
#include "../../gen-cpp/BeveragePreferenceService.h"

#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../logger.h"

namespace vending_machine{

class OrderBeverageServiceHandler : public OrderBeverageServiceIf {
 public:
  OrderBeverageServiceHandler(
      ClientPool<ThriftClient<WeatherServiceClient>> *weather_client_pool,
      ClientPool<ThriftClient<BeveragePreferenceServiceClient>> *beverage_client_pool);
  ~OrderBeverageServiceHandler() override=default;

  void PlaceOrder(std::string& _return, const int64_t city) override;

 private:
  ClientPool<ThriftClient<WeatherServiceClient>> *_weather_client_pool;
  ClientPool<ThriftClient<BeveragePreferenceServiceClient>> *_beverage_client_pool;
};

OrderBeverageServiceHandler::OrderBeverageServiceHandler(
    ClientPool<ThriftClient<WeatherServiceClient>> *weather_client_pool,
    ClientPool<ThriftClient<BeveragePreferenceServiceClient>> *beverage_client_pool) {

  _weather_client_pool = weather_client_pool;
  _beverage_client_pool = beverage_client_pool;
}

void OrderBeverageServiceHandler::PlaceOrder(std::string& _return, const int64_t city){
  printf("PlaceOrder function started\n");

  // Log and print to track weather client pool status
  printf("Popping weather client from pool...\n");
  auto weather_client_wrapper = _weather_client_pool->Pop();
  if (!weather_client_wrapper) {
    printf("Failed to connect to weather-service client\n");
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to weather-service";
    throw se;
  }
  printf("Weather client obtained from pool\n");

  auto weather_client = weather_client_wrapper->GetClient();
  WeatherType::type weatherType = WeatherType::type::COLD;

  try {
    printf("Calling GetWeather for city: %ld\n", city);
    weatherType = weather_client->GetWeather(city);
    printf("Received weather type: %d\n", weatherType);
  } catch (...) {
    _weather_client_pool->Push(weather_client_wrapper);
    LOG(error) << "Failed to call GetWeather";
    printf("Exception occurred while calling GetWeather\n");
    throw;
  }
  _weather_client_pool->Push(weather_client_wrapper);
  printf("Returned weather client to pool\n");

  // Determine beverage type based on weather
  BeverageType::type btype = (weatherType == WeatherType::type::WARM) ?
                              BeverageType::type::COLD : BeverageType::type::HOT;
  printf("Determined beverage type: %d\n", btype);

  // Log and print to track beverage client pool status
  printf("Popping beverage client from pool...\n");
  auto beverage_client_wrapper = _beverage_client_pool->Pop();
  if (!beverage_client_wrapper) {
    printf("Failed to connect to beverage-preference-service client\n");
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to beverage-preference-service";
    throw se;
  }
  printf("Beverage client obtained from pool\n");

  auto beverage_client = beverage_client_wrapper->GetClient();

  try {
    printf("Calling GetBeverage with type: %d\n", btype);
    beverage_client->GetBeverage(_return, btype);
    printf("Received beverage: %s\n", _return.c_str());
  } catch (...) {
    _beverage_client_pool->Push(beverage_client_wrapper);
    LOG(error) << "Failed to call GetBeverage";
    printf("Exception occurred while calling GetBeverage\n");
    throw;
  }
  _beverage_client_pool->Push(beverage_client_wrapper);
  printf("Returned beverage client to pool\n");

  

  LOG(info) << "Final beverage selected: " << _return;

}

} // namespace vending_machine

#endif //VENDING_MACHINE_MICROSERVICES_ORDERBEVERAGEHANDLER_H
