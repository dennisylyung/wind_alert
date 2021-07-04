# Wind Forecast Alert

This project checks the Hong Kong Observatory website for upcoming strong wind. 

If suitable wind conditions are found, an email alert will be sent using AWS SES. 

This project is to be deployed on AWS lambda. 

I developed this code so I won't miss good days for windsurfing. 

## Running locally

## Requirements

* Python 3.8+
* AWS credentials in the environment with SES permissions

### Installation

```shell
pip install -r requirements.txt
```

### Running

1.Set the environment variable `ALERT_RECIPIENT` to your email address
2. Run the Python file
    ```shell
    python lambda_function.py
    ```

## Deployment

Since the projects uses extra dependencies, 
you need a packaged application, or [a lambda layer](https://dev.to/mmascioni/using-external-python-packages-with-aws-lambda-layers-526o) to deploy it.
