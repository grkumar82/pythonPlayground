# Given three data structures, calculate effectiveness rate of each of the channels
# Also write test cases to test your code

from decimal import Decimal
from typing import NamedTuple
import unittest
from collections import defaultdict

class Bill_Example(NamedTuple):
    patient_id: str
    amount: Decimal
    create_timestamp: int


class Communication_Example(NamedTuple):
    patient_id: str
    channel_type: str
    sent_timestamp: int


class Payment_Example(NamedTuple):
    patient_id: str
    amount: Decimal
    paid_timestamp: int

class Effectiveness(NamedTuple):
    channel_type: str
    effectiveness: Decimal

BILLS = [
    Bill_Example("1a", Decimal(900), 170910 * 10000),
    Bill_Example("2b", Decimal(800), 170920 * 10000),
    Bill_Example("2b", Decimal(750), 170920 * 10000),    
]

COMMUNICATIONS = [
    Communication_Example("1a", "email", 170925 * 10000),
    Communication_Example("2b", "text", 170920 * 10000),
    Communication_Example("2b", "paper_mail", 170930 * 10000),
    Communication_Example("2b", "paper_mail", 170940 * 10000),        
]

PAYMENTS = [
    Payment_Example("1a", Decimal(100), 170926 * 10000),
    Payment_Example("2b", Decimal(350), 170945 * 10000),    
]

def calculate_effectiveness(communications, payments):
# Which channel is most effectiveness at driving payments
# Effectiveness is avg time taken to respond to a communication
# defaultdict -> one each for paper_mail, text and email; key is patient_id; value -> payment received & last time communique was sent
    
    pm, email, text = defaultdict(list), defaultdict(list), defaultdict(list)

    for items in communications:
        # email
        new_timestamp = int(int(items.sent_timestamp)/10000)
        if items.channel_type == 'email':
            if items.patient_id in email and email[items.patient_id][0] < new_timestamp:
                email[items.patient_id].pop()
                email[items.patient_id].append(new_timestamp)
            elif items.patient_id not in email:
                email[items.patient_id].append(new_timestamp)
        # paper_mail
        if items.channel_type == 'paper_mail':
            if items.patient_id in pm and pm[items.patient_id][0] < new_timestamp:
                pm[items.patient_id].pop()
                pm[items.patient_id].append(new_timestamp)
            elif items.patient_id not in pm:
                pm[items.patient_id].append(new_timestamp)    
        # text
        if items.channel_type == 'text':
            if items.patient_id in text and text[items.patient_id][0] < new_timestamp:
                text[items.patient_id].pop()
                text[items.patient_id].append(new_timestamp)
            elif items.patient_id not in pm:
                text[items.patient_id].append(new_timestamp)    

    for items in payments:
        new_timestamp = int(int(items.paid_timestamp)/10000)
        if items.patient_id in email and new_timestamp > email[items.patient_id][0]:
            email[items.patient_id].append(new_timestamp)
        if items.patient_id in text and new_timestamp > text[items.patient_id][0]:
            text[items.patient_id].append(new_timestamp)    
        if items.patient_id in pm and new_timestamp > pm[items.patient_id][0]:
            pm[items.patient_id].append(new_timestamp)


    email_time, text_time, pm_time = [], [], []

    for patient_id, times in email.items():
        email_time.append(times[1] - times[0])
    for patient_id, times in text.items():
        text_time.append(times[1] - times[0])
    for patient_id, times in pm.items():
        pm_time.append(times[1] - times[0])

    email_eff, text_eff, pm_eff = (sum(email_time)/len(email_time)), (sum(text_time)/len(email_time)), (sum(pm_time)/len(email_time))
    
    emails, texts, paper_mails = Effectiveness('email', email_eff), Effectiveness('text', text_eff), Effectiveness('paper_mail', pm_eff)
    return (emails, texts, paper_mails)

print(calculate_effectiveness(COMMUNICATIONS, PAYMENTS))

class EffectivenessTestCase(unittest.TestCase):
    def test_effectiveness(self):
        self.bills = BILLS
        self.communications = COMMUNICATIONS
        self.payments = PAYMENTS
        effectiveness_measures = calculate_effectiveness(COMMUNICATIONS, PAYMENTS)
        for i in range(len(effectiveness_measures)):
            if effectiveness_measures[i][0] == 'email':
                self.assertEqual(effectiveness_measures[i][1],0)
            

unittest.main()
