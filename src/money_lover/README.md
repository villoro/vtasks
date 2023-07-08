# vtasks.money_lover subflow

Since 2010, I have diligently recorded all my expenses and incomes. In recent years, I have been using [Money Lover](https://moneylover.me to manage my finances. The app provides an option to export the data as an `.xlsx`/`.csv` file, which I upload to Dropbox for further processing.

The `vtasks.money_lover` subflow is responsible for reading the exported `.xlsx`/`.csv`  file, processing the data, and exporting it as a cleaned `parquet` file. This standardized format allows for efficient storage and analysis of the financial data.

By automating the processing and cleaning of financial data, the `vtasks.money_lover` subflow ensures that the data is ready to be used in the [expensor](https://github.com/villoro/vtasks/tree/master/src/expensor) subflow. This integration streamlines the flow of financial information and enables seamless analysis and reporting in subsequent stages of the pipeline.
