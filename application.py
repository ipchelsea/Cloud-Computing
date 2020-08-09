
import urllib3
import boto3
import botocore
from flask import Flask, render_template, request, redirect, url_for,flash
from boto3.dynamodb.conditions import Key
from botocore.client import ClientError
import botocore.vendored.requests.packages.urllib3 as urllib3
import os
from urllib3 import PoolManager
import time

target_url = 'https://chelsfriedchickenloveryas637.s3.amazonaws.com/input.txt'
application = Flask(__name__)
application.secret_key = '7498348948'
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
dynamodb_client = boto3.client('s3', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')
bucket = 'chelsfriedchickenloveryas637'
key = 'input.txt'


@application.route('/', methods=['POST', 'GET'])
@application.route('/')
def default():
    if request.method == 'POST':
        arr = request.form
        if request.form['button'] == 'Query':

            # return request.form['FirstName'] + " " + request.form['LastName']

            first = request.form['FirstName']
            last = request.form['LastName']
            if not first and not last:
                flash('You forgot to enter a first and last name!')
                return render_template('base.html')

            # return redirect(url_for('error', error="You forgot to enter a first or last name!"))

            people = query(first, last)
      
            # return("Information for" + query(first,last))

            if not people:
                if not first:
                    flash("No one with the last name \'" + last
                          + "\' exists in the database!")
                elif not last:
                  
                    flash("No one with the first name \'" + first
                          + "\' exists in the database!")
                else:
                    flash("No one with the first name \'" + first
                          + "\' and last name \'" + last
                          + "\' exists in the database!")
            else:

                flash('RESULTS: \n')
                for x in people:
                    flash(x[0] + ' ' + x[1])
                    for attr in people[x]:
                        flash(attr)

        if request.form['button'] == 'Clear':
            clear()
            clear_s3()

        if request.form['button'] == 'Load':
            flash(load())

        if request.form['button'] == 'Return To Main Page':
            return render_template('base.html')
    return render_template('base.html')


def query(first, last):
    table = dynamodb.Table('people')
    print ('firstName' + first + 'lastName' + last)
    if not first or not last:
        if first:
                try:
                    response = table.query(
                        KeyConditionExpression=Key('FirstName').eq(first)
                    )
                    return formatNames(response)
                except Exception as e:
                    print(e)
                    return []
        elif last:

            try:
                response = table.query(IndexName='LastName-FirstName-index',
                                KeyConditionExpression=Key('LastName'
                                ).eq(last))
                return formatNames(response)
            except Exception as e:
                print (e)
                return []
    else:

        try:
            response = table.query(KeyConditionExpression=Key('FirstName'
                            ).eq(first) & Key('LastName').eq(last))
            return formatNames(response)
        except Exception as e:

            print (e)
            return []


def formatNames(response):
    aws_dict = {}
    for i in response['Items']:
        attributes = []
        for x in i:
            if x != 'FirstName' and x != 'LastName':
                attributes.append(x + ': ' + i[x])
        aws_dict[(i['FirstName'], i['LastName'])] = attributes
    return aws_dict


def create_DB():
    try:
        table = dynamodb.create_table(TableName=('people'),
                    KeySchema=[{'AttributeName': 'FirstName',
                    'KeyType': 'HASH'}, {'AttributeName': 'LastName',
                    'KeyType': 'RANGE'}],
                    AttributeDefinitions=[{'AttributeName': 'FirstName'
                    , 'AttributeType': 'S'},
                    {'AttributeName': 'LastName', 'AttributeType': 'S'
                    }], ProvisionedThroughput={'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5})

        print ('Table status:', table.table_status)
        waiter = dynamodb_client.get_waiter('table_exists')
        waiter.wait(TableName='people')
        return True
    except Exception as e:
        print(e)
        print ('Table does not exist')


def load():
    create_DB()
    data = awsCall()
    formatted = parseData(data)
    return update_DB(formatted)


def awsCall():
    http = PoolManager()
    r = http.request('GET', target_url)
    s3.put_object(ACL='public-read', Body=r.data, Bucket=bucket, Key=key, )
    return r.data


def clear():
    try:
        table = dynamodb.Table('people')

        scan = table.scan(ProjectionExpression='#pk, #sk',
                          ExpressionAttributeNames=
                          {'#pk': 'LastName',
                          '#sk': 'FirstName'}
                          )

        print (scan)
        for each in scan['Items']:
            print (each)

        with table.batch_writer() as batch:
            for each in scan['Items']:
                batch.delete_item(Key=each)

        flash('Successful cleared the table!')
    except Exception as e:

        flash('Something went wrong :( :' + str(e))


def clear_s3():
    try:
        s3.delete_object(Bucket=bucket, Key=key)
        flash('Cleared S3 Object!')
    except Exception as e:

        flash(str(e))


def update_DB(x):
    try:
        table = dynamodb.Table('people')
        for people in x:
            print (people)
            response = table.put_item(Item=people)
            return ('SUCCESSFULLY UPDATED')
    except Exception as e:

        print (e)
        if isinstance(e,
                      dynamodb_client.exceptions.ResourceNotFoundException):
            return 'This table is in the process of being cleared, please give it a minute!'
        return str(e)


def parseData(data):
    print ('-------------------LOADING DATA---------------------')
    case0 = data.split('\n')
    case1 = [x.strip() for x in case0]
    case2 = []
    case3 = []

    for x in case1:
        print (x)
        case2.append(x.split())

    for people in case2:
        if len(people) >= 2:
            x = {}
            x['FirstName'] = people[0]
            x['LastName'] = people[1]
            for attr in people[2:]:
                attr_val = attr.split('=')
                x[attr_val[0]] = attr_val[1]
            case3.append(x)

    return case3
    print ('-------------------DATA SUCCESSFULLY LOADED---------------------')


# --------------------------------------------------------------------------------------------------------

def testConnections(wait):
    time.sleep(wait)
    http = urllib3.PoolManager()
    try:
        r = http.request('GET', target_url)
        if r.status >= 400:
            if wait > 5:
                return False
        else:

            return True
    except:

        if wait > 5:
            return False
        return test_connections(wait + 1)


if __name__ == '__main__':
    application.run(debug=True)
