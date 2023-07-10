# MarketPrices
Scans daily prices from prominenet Israeli supermarkets to compare fruits and vegetables price fluctuation

## General overview
The program downloads daily the FullPrice files from supermarket chains. It downloads Stores files if a FullPrice file contains an unkown store.
Items of interest by manufacturer, specific item code or specific regex in the item code are logged into the db.
No automatic deletion of files is done.
Each supermarket chain has a class that is a subclass of Chain class subclass.
The supermarket chain class is thin to simplify initiation.
The chain subclass handels specific implementation differences between platforms, mainly login and page download.

## Chain picking
The program scanned 11 top supermarkets that are the biggest in their sector.
They should have wide network and be an houehold name.  
convenience store-like chains weren't included.

## Item and itemCodes
itemLinker.csv is a matrix assinging each of ~40 fruits and vegetables with an itemCode from a specific chain, planned for future analysis.
Picked f&v that are available all year round. 
When more than one itemCode was available picked the regular itemCode (no wrapped produce unless that is the standard way of selling).  
Different chains order different quality f&v. Preffered to use the AA quality, than the supreme and than other qualities
