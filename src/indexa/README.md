# vtasks.indexa subflow

The `vtasks.indexa` subflow is designed to extract the funds held in the robo-advisor called [indexa_capital](https://indexacapital.com/). It retrieves the relevant information about the invested money.

After extracting the data from indexa_capital, the subflow updates this information into Google Spreadsheets. These spreadsheets are then utilized in the [expensor](https://github.com/villoro/vtasks/tree/master/src/expensor) subflow, where the data from indexa is combined with other financial data sources.

By integrating the information from indexa into the overall financial analysis, the `vtasks.indexa` subflow ensures that the data from the robo-advisor is effectively utilized and contributes to a comprehensive understanding of personal finances. This integration streamlines the flow of information and enables efficient tracking and reporting of investments.
