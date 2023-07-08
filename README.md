# vtasks: Personal Pipeline
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)

This repo contains my personal pipeline and serves two main purpouses:

1. **Learning**: it serves as a playground for trying and learning new things. As an example I've used this for trying different orchestrators such as [Airflow](https://airflow.apache.org/), [Luigi](https://luigi.readthedocs.io/en/stable/) and [Prefect](https://www.prefect.io/opensource/) which has allowed me to deeply understand pros and cons of each.
2. **Automating**: this is a real pipeline that runs hourly in production and that allows me to automate certain repetitive tasks. You can find more details in [source](https://github.com/villoro/vtasks/tree/master/src).

## Pipeline design with Prefect


## Deployment

For production I'm using [Heroku](https://www.heroku.com/) (with the [Eco plan](https://www.heroku.com/pricing) at $5/month) since it greatly simplifies the CD (it has automatic deploys linked to changes to the `main` branch) and maintainace for a small fee. In the past I used AWS free tier but it was harder to maintain.

In terms of scheduling the pipeline is running hourly and usually takes 6-8 minutes to complete. So in order to not waste resources I'm using [Heroku Scheduler](https://devcenter.heroku.com/articles/scheduler) which allows me to trigger the pipeline with a cron.

## Tasks

### Expenses/Incomes Report

Since 2010 I have been recording all expenses and incomes. For the last years I have been using [Money Lover](https://moneylover.me/) to do so.
This app has a way to export the data as an Excel file. But the problem is that it does not have the format I want.
So the first task is to clean that data (`clean data`).

With the data cleaned the idea is to create a custom html report using **Jinja2** templates.

In order to make it more flexible there are two tasks:

1. `Extract info` that creates a `yaml` with the info
2. `Create Report` that creates the `html` report with the `yaml` data

With this approach is easy to modify the data or the template without needing to modify the other.

So the pipeline is as follows:

As you can see I use **dropbox** for storing all data this way is easy for me to access or modify it.

The report itself uses [W3css](https://www.w3schools.com/w3css/) for the layout and [Highcharts](https://www.highcharts.com/) for charts.

Here you can see some of the pages the report have:

![report_dashboard](images/report_1_dashboard.png)
![report_evolution](images/report_2_evolution.png)
![report_comparison](images/report_3_comparison.png)
![report_pies](images/report_4_pies.png)
![report_liquid](images/report_5_liquid.png)
![report_sankey](images/report_8_sankey.png)

And of course the report is **responsive**:

![report_nexus_5X](images/report_nexus_5X.png)


### Flights tracker

I regularly travel to Italy and I want to do it as cheap as possible.
So I thought the best way to do it was to let the assistant track all the prices between the airports I wanted and store that data.
Then I could use **Data Analysis** or **Machine Learning** to minimize the price I pay for the flights.

To do so I used the **Rapid API** [Flight Search](https://rapidapi.com/skyscanner/api/skyscanner-flight-search) app.
This API allowed me to query some pairs of airports daily for free.
So right now the assistant is storing a year of data each day so that I can see prices changes and which company offers cheaper flights each day.

![flights_pipeline](images/flights_task.png)

As you can see **Rapid API** is getting their data from **Skyscanner**.

A sample of the data:

![flights_data](images/flights_data.jpg)

## Author
* [Arnau Villoro](villoro.com)

## License
The content of this repository is licensed under a [MIT](https://opensource.org/licenses/MIT).
