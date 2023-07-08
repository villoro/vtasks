# vtasks.cryptos subflow

The `vtasks.cryptos` subflow is responsible for extracting the latest prices of cryptocurrencies from [Cryptocompare](https://www.cryptocompare.com/) Additionally, it extracts information about the funds stored in cryptocurrency exchanges.

The subflow then updates this information into Google Spreadsheets, which are utilized in the [expensor](https://github.com/villoro/vtasks/tree/master/src/expensor) subflow.

By extracting real-time cryptocurrency prices and integrating with exchanges, this subflow ensures that the cryptocurrency data is up-to-date and accurately reflected in the associated Google Spreadsheets. This allows for seamless integration with the [expensor](https://github.com/villoro/vtasks/tree/master/src/expensor) subflow, which utilizes this data for further analysis and reporting on personal finances.
