def format_amount(input_str):
    formatted_amt = input_str.split("$")[1]
    formatted_amt = float(formatted_amt.replace(",", ""))
    return formatted_amt


def parse_data_json(input_dtr):
    userId = input_dtr["userId"]
    userName = input_dtr["userName"]
    timestamp = input_dtr["timestamp"]
    txnType = input_dtr["txnType"]
    formatted_amt = format_amount(input_dtr["amount"])

    return [userId, userName, timestamp, txnType, formatted_amt]
