# Shoonya Options Chain
A simple program written in Python using PyQt to stream Stock options chain
from Finvasia Shoonya using their Python API package.

At present following functionality is supported:
 - Download and parse FNO master file 
 - Display the list of FNO stocks
 - Display the option chain of the selected stock
 - Display price updates for the selected stock's option chain if the user is logged in.
 - Display current open position with avg buy price, ltp, p/l, return %


## Installation
To use the program follow the steps below:

```bash
git clone https://github.com/trivalent/shoonya_optionchain.git
cd shoonya_optionchain
pip install -r requirements.txt
