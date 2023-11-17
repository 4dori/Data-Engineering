#export all authentication variables from .env file
set -a
source .env
set +a

#import authentication variables to main.tf (terraform)
template=$(cat main-temp.tf)
parsed=$(envsubst <<< "$template")
echo "$parsed" > ./terraform/main.tf

# for giving execute permission to sh file use command:
# chmod a+x terraform.sh