# High Frequency Spoofing Manipulation 
### Overview of what I did during summer 2023 while interning as a Quantitative Researcher at Sun Zu Lab. With Timothée Fabre, I developed a cost-function based approach at detecting spoofing manipulation in digital assets centralized exchanges (Binance, CoinBase, etc...)

## Overview of the directory 
- Preliminary work on execution probability of an order given multiple parameters
- Cost function of the maker spoofer agent. Caluclations were run on overnight distrubuted cloud CPUs.

## But what is Spoofing ? 

Let's consider the spoofer maker who wants to buy an asset through the placement of a limit order at best bid. To increase his execution probabilities, he places a big limit order on the ask side further in the book to modify the imbalance and increase the execution probability of his initial order. 
Plenty of question arise : How big should the order be ? Where should he be placed ? What market parameters allow th strategy to work (or not ?) Check the PDF to get answers !




