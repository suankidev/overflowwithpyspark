import unicodedata


class FixedFloat:
    inr_value = {"EUR": 108, "USD": 80}

    def __init__(self, currency):
        self.currency = currency

    @classmethod
    def from_amount(cls, val1, val2):
        return cls(val1 + val2)

    def current_value(cls, userentry):
        if userentry[0] == "INR":
            return cls(userentry[1])
        else:
            present_inr_value = userentry[1] * cls.inr_value[userentry[0]]
            return cls(present_inr_value)

    def __repr__(self):
        return f"<FixedFloat {self.currency: .2f}"


class Euro(FixedFloat):
    def __init__(self, currency):
        super().__init__(currency)
        self.symbol = unicodedata.lookup("EURO SIGN")

    def __repr__(self):
        return f"Euro {self.symbol} {self.currency: .2f}"


b = Euro(23849.3794)
print(b)


def getCurrencyType():
    currency_type = input("Enter currency type: ")
    currency = input("Enter currency value: ")
    return currency_type, currency


available_currency = ["INR", "US", "GBP", "EUR"]

def main():
    user_input = getCurrencyType()
    flag = True
    while flag:
        if user_input[0] in available_currency:
            flag = False


if __name__ == "__main__":
    """run the currency converter programm"""
    main()
