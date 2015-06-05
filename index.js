var fs = require('fs');
var aws = require('aws-sdk');
var query = require("pg-query");
var limit = require("simple-rate-limiter");
var propertiesReader = require('properties-reader');
var properties = propertiesReader('properties.file');
var dynamodb = new aws.DynamoDB();

// properties
var AWS_ACCESS_KEY_ID = properties.get('aws.access.key');
var AWS_SECRET_ACCESS_KEY = properties.get('aws.secret.key');
var AWS_DYNAMODB_TABLE = properties.get('aws.dynamodb.table');
var AWS_BUCKET_LOGS = properties.get('aws.s3.bucket.logs');

// postgresql connection
query.connectionParameters = properties.get('aws.postgres.endpoint');

// set AWS configuration for future requests
aws.config.update({"accessKeyId": AWS_ACCESS_KEY_ID, "secretAccessKey": AWS_SECRET_ACCESS_KEY, "region": "eu-west-1"});
aws.config.apiVersions = {
  dynamodb: '2012-08-10'
};

// functions
function logDebug(message) {
	fs.appendFile('./logs/debug.txt', new Date().getTime() + ": " + message + "\n", function (err) {
		if (err) {
			throw err;
		}
	});
}

function logError(message) {
	fs.appendFile('./logs/errors.txt', new Date().getTime() + ": " + message + "\n", function (err) {
		if (err) {
			throw err;
		}
	});
}

function uploadLogs() {
	fs.exists('./logs/debug.txt', function (exists) {
		if (exists) {
			var today = new Date();
			var body = fs.readFileSync('./logs/debug.txt');
			var key = today.getUTCDate() + "-" + (today.getUTCMonth() + 1) + "-" + today.getUTCFullYear() + "_" + (("0" + today.getUTCHours()).slice(-2)) + ":" + (("0" + today.getUTCMinutes()).slice(-2)) + "_debug.txt";
			var s3 = new aws.S3({
				params : {
					Bucket : AWS_BUCKET_LOGS,
					Key : key
				}
			});
			
			s3.upload({Body : body}, function(err, data) {
				if (!err) {
					fs.unlinkSync('./logs/debug.txt');
				} else {
					logError("UPLOAD DEBUG LOGS TO S3 ERROR: " + err);
				}
			});
		}
	});
	
	
	fs.exists('./logs/errors.txt', function (exists) {
		if (exists) {
			var today = new Date();
			var body = fs.readFileSync('./logs/errors.txt');
			var key = today.getUTCDate() + "-" + (today.getUTCMonth() + 1) + "-" + today.getUTCFullYear() + "_" + (("0" + today.getUTCHours()).slice(-2)) + ":" + (("0" + today.getUTCMinutes()).slice(-2)) + "_errors.txt";
			var s3 = new aws.S3({
				params : {
					Bucket : AWS_BUCKET_LOGS,
					Key : key
				}
			});
			
			s3.upload({Body : body}, function(err, data) {
				if (!err) {
					fs.unlinkSync('./logs/errors.txt');
				} else {
					logError("UPLOAD ERRORS LOGS TO S3 ERROR: " + err);
				}
			});
		}
	});
}

function deleteExpiredVisits(time) {
	query("DELETE FROM visits WHERE expires_at < $1", [time], function(err, rows, result) {
		if (!err) {
			calculateCurrentStats();
		} else {
			logError("Delete items in postgresql failed: " + err);
		}
	});
}

function calculateCurrentStats() {
	query("SELECT club_id, round(avg(age),2) age, (count(CASE WHEN is_male THEN 1 ELSE null END) * 100) / count(*) male FROM visits GROUP BY club_id", function(err, rows, result) {
		if (!err) {
			if (rows.length > 0) {
				rows.forEach(function(item) {
					updateClubPG(item.club_id, item.age, item.male);
				});
			}
		} else {
			logError("Selection of current stats from postgresql failed: " + err);
		}
	});
}

function updateClubPG(id, age, male) {
	query("UPDATE clubs SET ratio_male = $1, average_age = $2 WHERE id = $3", [male, age, id], function(err, rows, result) {
		if (!err) {
			updateApi(id, age, male);
		} else {
			logError("Current stats update from postgresql failed: " + err);
		}
	});
}

// update item api
var updateApi = limit(function(id, age, male) {
	dynamodb.updateItem({
    	"Key": {
	    	"Id": id
    	},
		TableName: AWS_DYNAMODB_TABLE,
	    "UpdateExpression": "SET RatioMale = :a, AverageAge = :b",
	    "ExpressionAttributeValues" : {
	    	":a" : {"N":male},
	    	":b" : {"N":age}
	    }
	}, function(err, data) {
	  	if (err) {
				logError("Update item to dynamodb failed: " + err);
			}
	});
}).to(5).per(1000);

// main execution
deleteExpiredVisits(new Date(new Date().setDate(new Date().getDate() - 7)).getTime());
