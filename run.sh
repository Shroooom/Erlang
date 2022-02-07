#!/usr/bin/env bash

# Compile code
erlc main.erl util.erl

# Run my program.
input=$1

files=()
while IFS= read -r line
do
    args=($line)
    case ${args[0]} in
        d)
            erl -noshell -detached -sname ${args[1]} -setcookie foo -eval "main:start_dir_service()"
            sleep 1
            ;;
        f)
            erl -noshell -detached -sname ${args[2]} -setcookie foo -eval "main:start_file_server(${args[1]})"
            ;;
        c)  
            erl -noshell -detached -sname client@localhost -setcookie foo -eval "main:create(${args[1]}, ${args[2]})"
            sleep 1
            pkill -f client@localhost
            sleep 1
            ;;
        g)
            temp=${args[2]}
            temp="${temp#\'}"
            temp="${temp%\'}"
            files+=($temp)
            erl -noshell -detached -sname client@localhost -setcookie foo -eval "main:get(${args[1]}, ${args[2]})"
            sleep 1
            pkill -f client@localhost
            sleep 1
            ;;
        q)
            erl -noshell -detached -sname client@localhost -setcookie foo -eval "main:quit(${args[1]})"
            sleep 1
            pkill -f client@localhost
            sleep 1
            ;;
        sleep)
            sleep ${args[1]}
    esac
done < "$input"

for value in "${files[@]}"
do
    file1="input/$value"
    file2="downloads/$value"
    echo $file1
    echo $file2
    if cmp -s "$file1" "$file2"; then
        printf "The files %s and %s are the same\n" "$file1" "$file2"
    else
        printf "The files %s and %s are different\n" "$file1" "$file2"
    fi    
done
