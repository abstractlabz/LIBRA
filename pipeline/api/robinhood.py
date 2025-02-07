import robin_stocks.robinhood as r
import pyotp

totp = pyotp.TOTP("My2factorAppHere").now()
print("Current OTP: ", totp)
login = r.login("team@fineas.ai","Kobby1205!", mfa_code=totp)
positions_data = r.build_holdings()
print(positions_data)










