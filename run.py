import scrape
from includes.debug import *


# Using @property decorator
class Celsius:
    def __init__(self, temperature=None):
        object.__setattr__(self, 'temperature', None)
        if temperature is not None:
            self.temperature = temperature
        pvdd(self.temperature)

        # setattr(self, 'temperature', -99999)

    def to_fahrenheit(self):
        return (self.temperature * 1.8) + 32

    @property
    def temperature(self):
        print("Getting value..." + str(self._temperature))
        return self._temperature

    @temperature.setter
    def temperature(self, value):
        if value is None:
            raise ValueError('Temp cannot be none!')
        print("Setting value...")
        if hasattr(self, '_temperature'):
            print("Current: " + str(self._temperature))
        if value < -273.15:
            raise ValueError("Temperature below -273 is not possible, your value: " + str(value))
        self._temperature = value
        print("Value set to: " + str(self._temperature))


# create an object
human = Celsius()
print(human.temperature)
print(human.to_fahrenheit())
coldest_thing = Celsius(-300)

if __name__ == "__main__":
    scrape.main()
