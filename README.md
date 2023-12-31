# LambdaFuzzer

## Background
LambdaFuzzer is a security research and QA tool for fuzzing urls at scale. This tool is capable of testing millions of urls an hour.

LambdaFuzzer initializes a collection of lambda functions, repeatedly generates lists of candidate urls, and sends the candidate urls to the lambda functions to try. Running the url connections from the lambda functions has a number of benefits:
- Network throughput scales with the number of lambda functions deployed
- Lambda functions rotate IP addresses, which can improve privacy



## Setup Proxies
First, run `pip install requirements.txt` to install python dependencies.


Next, we will instantiate the proxies for fuzzing. 

### Configure AWS
```
aws configure --profile danshiebler # start by creating a profile, which gets written to ~/.aws/config 
```

### init terraform
Set the number of proxies in `lambda_fuzzer/variables.tf` and run:
```
cd lambda_scraper
terraform init
```

### To tear down and rebuild
```
terraform apply -destroy -auto-approve
terraform apply -auto-approve
```

## Run the tool
Run the following command to test every parameter in `path_to_fuzz_terms_file` against `url_template` and write the urls that resolve without errors to `s3_bucket_name`.
```
 python run_fuzzer.py \
    --url_template_list=https://%s.github.io,https://%s.pages.dev \
    --s3_path_list=s3://bucket-name/github,s3://bucket-name/gpages \
    --path_to_fuzz_terms_file=<path to the text file of fuzzing terms> \
    --aws_profile_name=<the name of the aws profile to use> \
    --s3_directory_key=<the key within the s3 bucket to write to> 
```
