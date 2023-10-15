# AWS Lambda Proxy-Based Subdomain Fuzzer

## Background
This is a tool to fuzz url parameters at scale. The tool uses a collection of AWS lambda functions as proxies to send get requests to a target url with a range of user-specified parameters. 

## Setup Proxies
First, run `pip install requirements.txt`. Next, we will instantiate the proxies for fuzzing. You can set the number of proxies in lambda_fuzzer/variables.tf

### Configure AWS
aws configure --profile danshiebler # start by creating a profile, which gets written to ~/.aws/config 

### init terraform
cd lambda_scraper
terraform init

### To tear down and rebuild
terraform apply -destroy -auto-approve
terraform apply -auto-approve

## Run the tool
```
 python run_fuzzer.py \
    --path_to_fuzz_terms_file=<path to the text file of fuzzing terms> \
    --aws_profile_name=<the name of the aws profile to use> \
    --url_template=<the template of the url to use, with `%s` in place of the term to fuzz> \
    --s3_bucket_name=<the bucket on s3 to write the text files of urls that returned 200 codes> 
    --s3_directory_key=<the key within the s3 bucket to write to> 
```
