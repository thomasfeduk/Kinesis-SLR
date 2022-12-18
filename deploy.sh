#!/bin/bash

rm deploy.zip
zip -r deploy.zip *
aws lambda update-function-code --function-name kworker --zip-file fileb://deploy.zip