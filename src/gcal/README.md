# vtasks.gcal subflow

The `vtasks.gcal` subflow is responsible for extracting data from Google Calendar using the [Google Calendar Simple API](https://github.com/kuzmoyev/google-calendar-simple-api). This subflow retrieves calendar data, allowing me to analyze and track how I spend my time.

Using the extracted data, the subflow generates reports that provide insights into how I allocate my time. For example, the reports may show the distribution of activities across different calendar types, as illustrated in the following image:

![gcal_report](/images/gcal_report.png)

To ensure accuracy and consistency, I track different activities in separate calendars based on their type. This allows for a more detailed and comprehensive analysis of my time allocation.

Additionally, the `vtasks.gcal` subflow includes an email report feature. Every Monday, it sends a summary of the previous week's activities via email. This provides a convenient and regular overview of my time utilization.

Furthermore, the subflow checks the description of each calendar entry to identify potential discrepancies. For instance, if an entry for "climbing" is typically associated with the "sports" calendar but appears in the "work" calendar, it triggers an email alert. This helps me identify and resolve any inconsistencies or confusion in my calendar entries.

By leveraging Google Calendar data and implementing these features, the `vtasks.gcal` subflow enables efficient tracking, reporting, and analysis of my time allocation while providing alerts for potential discrepancies.
