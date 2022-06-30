import time
import traceback
import datetime
from pathlib import Path
from typing import Optional
#https://medium.com/coinmonks/defi-protocol-data-how-to-query-618c934dbbe2
import requests
from concurrent.futures import ThreadPoolExecutor
from pandas.core.dtypes.generic import ABCSeries
import filecache
from globals import *
from utils import *
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import json

from string import Template


BASE_URL = 'https://api.thegraph.com/subgraphs/name/aave/protocol-v2'

_transport = RequestsHTTPTransport(
    url= BASE_URL,
    use_json=True
)

RAY = 10**27
SECONDS_PER_YEAR = 31536000

class Aave:
    #Aave class to act as Aave's API Client
    #All requests will be made through this class
    def __init__(self):
        #initialize object
        self.session = requests.Session()
        #Session object allows users to persist certain parameters across requests

    def _send_message(self, raw_query):
        client = Client(
            transport=_transport,
            fetch_schema_from_transport=True,
        )
        query = gql( raw_query )
        query_output = client.execute(query)
        return query_output

    def _liquidationCalls(self, number, collateralAmount, principalAmount, timestamp):
        raw_query = """
        {
            liquidationCalls (first: $number, where:{ collateralAmount_gt:$collateralAmount, principalAmount_gt:$principalAmount, timestamp_gt:$timestamp})
            {
                collateralReserve {
                    symbol
                    name
                    price {
                        priceInEth
                    }
                }
                principalReserve {
                    symbol
                    name
                    price {
                        priceInEth
                    }
                }
                    timestamp
                    collateralAmount
                    principalAmount
            }
        }
        """
        raw_query = Template(raw_query).substitute(
            number = number,
            collateralAmount = collateralAmount,
            principalAmount = principalAmount,
            timestamp = timestamp
        )
        return self._send_message(raw_query)

    def _flashLoans(self, number, amount, timestamp):
        raw_query = """
          { 
            flashLoans (first: $number, where: {amount_gt:$amount, timestamp_gt:$timestamp} ){
              reserve{
                name,
                symbol,
                price {
                  priceInEth
                }
              }
              amount,
              totalFee,
              initiator {
                id
              }
              timestamp
            }
          }
        """
        raw_query = Template(raw_query).substitute(
            number = number,
            amount = amount,
            timestamp = timestamp
        )
        return self._send_message(raw_query)

    def _repays(self, number, amount, timestamp):
        raw_query = """
          { 
            repays (first: $number, where:{ amount_gt:$amount, timestamp_gt: $timestamp}){
                amount
                user {
                    id
                  }
                onBehalfOf {
                    id
                }
                    reserve {
                      symbol
                      name
                      price {
                        priceInEth
                      }
                      utilizationRate
                      lastUpdateTimestamp
                }
              }
            }
            """
        raw_query = Template(raw_query).substitute(
            number = number,
            amount = amount,
            timestamp = timestamp
        )
        return self._send_message(raw_query)

   def _deposits (self, number, amount, timestamp):
        raw_query = """
          { 
            deposits (first: $number, where:{ amount_gt:$amount, timestamp_gt: $timestamp}){
                amount
                user {
                    id
                  }
                onBehalfOf {
                    id
                }
                reserve {
                    symbol
                    name
                    price {
                        priceInEth
                    }
                    utilizationRate
                    lastUpdateTimestamp
                }
              }
            }
            """
        raw_query = Template(raw_query).substitute(
            number = number,
            amount = amount,
            timestamp = timestamp
        )
        return self._send_message(raw_query)

   def _borrows (self, number, amount, timestamp):
        raw_query = """
          { 
            borrows (first: $number, where:{ amount_gt:$amount, timestamp_gt: $timestamp}){
                amount
                user {
                    id
                  }
                onBehalfOf {
                    id
                }
                    reserve {
                      symbol
                      name
                      price {
                        priceInEth
                      }
                      utilizationRate
                      lastUpdateTimestamp
                }
              }
            }
            """
        raw_query = Template(raw_query).substitute(
            number = number,
            amount = amount,
            timestamp = timestamp
        )
        return self._send_message(raw_query)


