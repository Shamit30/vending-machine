#ifndef VENDING_MACHINE_MICROSERVICES_BEVERAGEPREFERENCEHANDLER_H
#define VENDING_MACHINE_MICROSERVICES_BEVERAGEPREFERENCEHANDLER_H

#include <iostream>
#include <string>
#include <vector>
#include <cstdlib>
#include <ctime>

#include "../../gen-cpp/BeveragePreferenceService.h"
#include "../logger.h"

namespace vending_machine {

class BeveragePreferenceServiceHandler : public BeveragePreferenceServiceIf {
 public:
  BeveragePreferenceServiceHandler();
  ~BeveragePreferenceServiceHandler() override = default;

  void GetBeverage(std::string& _return, const BeverageType::type btype) override;
};

BeveragePreferenceServiceHandler::BeveragePreferenceServiceHandler() {
  std::srand(std::time(nullptr));  // seed for random generator
}

void BeveragePreferenceServiceHandler::GetBeverage(std::string& _return, const BeverageType::type btype) {
  std::vector<std::string> hot = {"cappuccino", "latte", "espresso"};
  std::vector<std::string> cold = {"lemonade", "ice tea", "soda"};

  const std::vector<std::string>& choices = (btype == BeverageType::type::HOT) ? hot : cold;
  int index = rand() % choices.size();
  _return = choices[index];

  LOG(info) << "Selected beverage: " << _return;
}

}  // namespace vending_machine

#endif // VENDING_MACHINE_MICROSERVICES_BEVERAGEPREFERENCEHANDLER_H
