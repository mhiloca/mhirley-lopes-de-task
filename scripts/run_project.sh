set -e

python src/fetch_nyt_data.py

dbt run -s tag:facts
dbt run -s tag:results
friends=(pete jake)

for friend in ${friends[*]}
    do
        dbt run -s result_friends_books --vars '{"friend":"'"$friend"'"}' 
done
