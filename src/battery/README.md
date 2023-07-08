# vtasks.battery subflow

The `vtasks.battery` subflow maintains a historical log of my phone battery. It reads the `.txt` files extracted from [3C Battery Manager](https://play.google.com/store/apps/details?id=ccc71.bmw) and exports it into a single `parquet` file. Also since the app only keeps 30 days of historical data it will send an email whenever the latest extraction is older than 20 days.
