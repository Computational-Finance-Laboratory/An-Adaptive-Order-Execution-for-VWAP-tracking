# Clean code

## DataExtract\.py

Extract raw data to readable dataframe

### Incode config:

* input_path  : path of raw input data
* output_path : path of extracted data
* Ncores : number of processing cores

### Input Files require

```
-- input_path
    |-----feed-mbl-YYYYMM
    |       |-------- feed-mbl-YYYYMMDD-orderbook.txt
    |-----feed-mbl-YYYYMM
    |       |-------- feed-mbl-YYYYMMDD-orderbook.txt
    |-----feed-trade-YYYYMM
    |       |-------- feed-trade-YYYYMMDD-orderbook.txt
    |-----feed-trade-YYYYMM
            |-------- feed-trade-YYYYMMDD-orderbook.txt

```

### Output Files
```
-- output_path
    |-----YYYYMMDD
    |        |----csv
    |        |     |-----symbol_id_auction.csv # ex. AAV_4262_auction.csv 
    |        |     |-----symbol_id_trade.csv # ex. AAV_4262_trade.csv
    |        |     |-----symbol_id_book.csv # ex. AAV_4262_book.csv
    |        |     |-----orderbook.csv # only have one file in this |directory
    |        | 
    |        |----pickle
    |              |-----symbol_id_auction.dat # ex. AAV_4262_auction.dat
    |              |-----symbol_id_trade.dat # ex. AAV_4262_trade.dat
    |              |-----symbol_id_book.dat # ex. AAV_4262_book.dat
    |              |-----orderbook.dat # only have one file in this directory
    |
    |
    |-----cache ( only have 1 directory , can delete after complete )

```

### Output format ( column name )
#### Auction File ( message number 62 in EMAPI )
    Event Time     
    ID ( 62 )
    Sequence number 
    Timestamp
    OrderbookID
    Imbalance
    AuctionPrice
    ResumeTime
    MatchQuantity
    IsFinal
#### Book File ( message number 140 in EMAPI )
    Event Time     
    ID  ( 140 )
    SeqNumber
    Timestamp
    OrderbookID
    isBid
    Price ( 9223372036854.775807 if ATO_bid , 0.000001  if ATO_offer )
    Volume
    EventType
#### Trade file ( message number 49 in EMAPI )

    Event Time     
    ID ( 49 )
    SeqNumber
    Timestamp
    OrderbookID
    EventType1
    EventType2
    TradeTime
    Volume
    price
    TradeID
    DealID
    isTBL
    TotalVolume
    TotalTurnOver
    LastRefPrice
    AvgPrice
    OpenPrice
    TotalTradeReportVolume
    TotalTradeReportTurnOver
    TotalNTrades
    TradeReportType
#### Orderbook file ( message number 296 in EMAPI )
    Event Time     
    ID ( 296 )
    Timestamp
    Name
    Persistant Name
    Current Name
    Market
    MarketList
    Segment
    close price 
    OrderbookID
    ValidDate
    Security type
    isOdd
    CeilingPrice
    FloorPrice
    Last Trade Time

## GenerateEvent.py
Generate event file ( how orderbook change overtime )

Generate trade2 file ( similar to trade file but show side and relation to event file )

### Incode config:

* input_path  : path of input data ( normally same path with output_path in DataExtract.py )

### Input Files require
use only pickle file

```
-- input_path
    |-----YYYYMMDD
            | 
            |----pickle
                  |-----symbol_id_auction.dat # ex. AV_4262_auction.dat
                  |-----symbol_id_trade.dat # ex. AV_4262_trade.dat
                  |-----symbol_id_book.dat # ex. AV_4262_book.dat
                  |-----orderbook.dat # only have one ile in this directory

```

### Output Files
```
-- output_path
    |-----YYYYMMDD
            |----csv
            |     |-----symbol_id_event.csv # ex. AAV_4262_event.csv 
            |     |-----symbol_id_trade2.csv # ex. AAV_4262_trade2.csv
            | 
            |----pickle
            |     |-----symbol_id_event.dat # ex. AAV_4262_event.dat
            |     |-----symbol_id_trade2.dat # ex. AAV_4262_trade2.dat

```

### Output format ( column name )

#### event file
    Timestame
    Price
    IsBid
    Change  
    Flag1
    Flag2
    Mark
    SeqNumber

#### Trade2 file

    Event Time
    Id
    SeqNumber
    Timestamp
    OrderbookID
    EventType1
    EventType2
    TradeTime
    Volume
    Price
    isTBL
    TotalVolume
    TotalTurnOver
    LastRefPrice
    TotalNTrades
    TradeReportType
    OrderbookRef
    OrderbookRef2
    Side




### Example output
#### Exmaple of event file
```
Timestamp	        Price	IsBid	      Change     Flag1 Flag2	   Mark	   SeqNumber
2017-12-07 09:59:27	 6.05	T	     200000	 UPDATE	 LO	     0	    101584900
2017-12-07 09:59:32	 6.45	F	     20000	 UPDATE	 LO	     0	    102092570
2017-12-07 09:59:42	 6.3 	F	     1000	 UPDATE	 LO	     0	    102944150
2017-12-07 09:59:47	 6.25	F	     -25000	 UPDATE	 MO	     0	    103412830
2017-12-07 09:59:50	 6.25	F	     -50000	 UPDATE	 MO	     0	    103663410
2017-12-07 09:59:51	 6.3	F	     10000	 UPDATE	 LO	     0	    103754790
2017-12-07 09:59:51	 6.3  	F	     10000	 UPDATE	 LO	     0	    103779050
2017-12-07 09:59:59	 6.2	T	     25000	 UPDATE	 LO	     0	    104496600
2017-12-07 09:59:59	 6.45	F	     5000	 UPDATE	 LO	     0	    104520620
2017-12-07 10:00:01	 6.35	F	     5000	 UPDATE	 LO	     0	    104711710
2017-12-07 10:00:05	 6.25	F	     -20000	 UPDATE	 MO	     0	    105133630
 
```
#### Example of trade2 file ( select only necessary columns )
```
SeqNumber	      Timestamp	       EventType2      Volume	Price  isTBL	  OrderbookRef  OrderbookRef2	Side
96952230	2017-12-07 09:58:55	AUCTION  	93300	6.25	 T		                          A
103412660	2017-12-07 09:59:47	T_TO_T  	25000	6.25		   103412830		          B
103663240	2017-12-07 09:59:50	T_TO_T  	50000	6.25		   103663410		          B
105133460	2017-12-07 10:00:05	T_TO_T  	20000	6.25		   105133630		          B
105218110	2017-12-07 10:00:06	T_TO_T  	50000	6.25		   105218280		          B

```
