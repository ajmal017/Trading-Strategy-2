# capstone-project
### Implementation of R-BREAKER & ATR Trading Strategy on Python Platform 

**Version 1.0.0**

The project is to implement a distributed Python platform that could be used to test a quant models for trading financial instruments in a network setting under client/server infrastructure. The client side includes:

1. Market data feed
2. Build Model
3. Back testing
4. Simulated Trading
5. Web Dashboard Implementation

The server is a multi-thread application which coordinates among all the client applications. Its main purposes are:
1. messaging among all the participants in json format
2. maintain a market participant list, and the list of stocks traded by participants
3. generate a consolidated order book for all the participants
4. create a simulated market for 30 days based on last 1 months of market data
---

## Contributors

- Siyuan Zou <sz2513@nyu.edu>

---

## License & copyright

@ Siyuan Zou, NYU Tandon Financial and Risk Engineering
