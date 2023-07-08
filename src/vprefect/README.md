# vtasks.vprefect subflow

The `vtasks.vprefect` subflow is responsible for extracting information about flow runs and task runs from [Prefect](https://www.prefect.io/). This includes both the main flows and any subflows, as subflows are also treated as flows within `Prefect`.

The extracted data is then stored in a `parquet` file, allowing me to maintain a comprehensive history of flow runs and task runs. This is particularly useful because Prefect only keeps the latest week's information for free tier.

Using this extracted data, the subflow generates a report that provides insights into pipeline failures and average flow durations. The report highlights any failures or errors that occurred during the flow runs, as well as the average duration of each flow.

An example of the report is shown below:

![vprefect_report](images/vprefect_report.png)

By extracting and analyzing the flow run and task run information, the `vtasks.vprefect` subflow enables me to monitor the performance and stability of the pipelines created using Prefect. It helps me identify any issues or bottlenecks and provides valuable insights into the overall pipeline execution.
