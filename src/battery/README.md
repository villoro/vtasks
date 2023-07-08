# vtasks.battery subflow

The `vtasks.battery` subflow is responsible for maintaining a historical log of my phone battery. It reads the `.txt` files extracted from [3C Battery Manager](https://play.google.com/store/apps/details?id=ccc71.bmw) and exports the data into a single `parquet` file.

Additionally, since the app only keeps 30 days of historical data, the subflow includes a notification system. If the latest extraction is older than 20 days, it sends an email to alert the user.

This subflow ensures that a comprehensive log of phone battery data is maintained in a structured format and provides timely notifications to prevent data loss due to expiration of the app's historical data storage.
