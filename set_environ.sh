input=".env"
while IFS= read -r line
do
  export $line
  # heroku config:set $line
done < "$input"

# Set 1password stuff
eval $(op signin)
export AWS_SECRET_ACCESS_KEY=$(op get item 4tqv5g2ptfysgkailaofdmgk5e --fields credential)

# aws configure --profile danshiebler
# Also sync database url
# export DATABASE_URL=$(heroku config:get DATABASE_URL)
# heroku config