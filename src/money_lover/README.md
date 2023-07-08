# vtasks.money_lover subflow

Since 2010 I have been recording all expenses and incomes. For the last years I have been using [Money Lover](https://moneylover.me/) to do so.
This app has a way to export the data as an `.xlsx`/`.csv` file which I upload it to `dropbox`. This `subflow` reads the `.xlsx`/`.csv`, processes it and exports it as a cleaned `parquet`.