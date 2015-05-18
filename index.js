var fs = require('fs');
var limit = require("simple-rate-limiter");
var propertiesReader = require('properties-reader');
var properties = propertiesReader('properties.file');

// requires multiple instances of pg-query (one for each different environment)
var query = require("pg-query");
delete require.cache[require.resolve("pg-query")];
var query2 = require("pg-query");
delete require.cache[require.resolve("pg-query")];
var query3 = require("pg-query");

// requires multiple instances of aws-sdk (one for each different environment)
var aws = require('aws-sdk');
delete require.cache[require.resolve("aws-sdk")];
var aws2 = require('aws-sdk');
delete require.cache[require.resolve("aws-sdk")];
var aws3 = require('aws-sdk');

var dynamodb = new aws.DynamoDB();
var dynamodb2 = new aws2.DynamoDB();
var dynamodb3 = new aws3.DynamoDB();

// properties
var AWS_ACCESS_KEY_ID_PRODUCTION = properties.get('aws.access.key.production');
var AWS_SECRET_ACCESS_KEY_PRODUCTION = properties.get('aws.secret.key.production');
var AWS_ACCESS_KEY_ID_STAGE = properties.get('aws.access.key.stage');
var AWS_SECRET_ACCESS_KEY_STAGE= properties.get('aws.secret.key.stage');
var AWS_ACCESS_KEY_ID_DEVELOPMENT = properties.get('aws.access.key.development');
var AWS_SECRET_ACCESS_KEY_DEVELOPMENT = properties.get('aws.secret.key.development');
var AWS_DYNAMODB_TABLE = properties.get('aws.dynamodb.table');
var AWS_BUCKET_NAME_LOGS = properties.get('aws.s3.bucket.name.logs');

// postgresql connections
query.connectionParameters = properties.get('aws.postgres.endpoint.production');
query2.connectionParameters = properties.get('aws.postgres.endpoint.stage');
query3.connectionParameters = properties.get('aws.postgres.endpoint.development');

// other variables
var STAGE = 'STAGE';
var PRODUCTION = 'PRODUCTION';
var DEVELOPMENT = 'DEVELOPMENT';

// set AWS configuration for future requests
aws.config.update({"accessKeyId": AWS_ACCESS_KEY_ID_PRODUCTION, "secretAccessKey": AWS_SECRET_ACCESS_KEY_PRODUCTION, "region": "eu-west-1"});
aws.config.apiVersions = {
  dynamodb: '2012-08-10'
};

// set AWS configuration for future requests
aws2.config.update({"accessKeyId": AWS_ACCESS_KEY_ID_STAGE, "secretAccessKey": AWS_SECRET_ACCESS_KEY_STAGE, "region": "eu-west-1"});
aws2.config.apiVersions = {
  dynamodb: '2012-08-10'
};

// set AWS configuration for future requests
aws3.config.update({"accessKeyId": AWS_ACCESS_KEY_ID_DEVELOPMENT, "secretAccessKey": AWS_SECRET_ACCESS_KEY_DEVELOPMENT, "region": "eu-west-1"});
aws3.config.apiVersions = {
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
					Bucket : AWS_BUCKET_NAME_LOGS,
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
					Bucket : AWS_BUCKET_NAME_LOGS,
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
			calculateCurrentStats(PRODUCTION);
		} else {
			logError("Delete items in postgresql PRODUCTION failed: " + err);
		}
	});

	query2("DELETE FROM visits WHERE expires_at < $1", [time], function(err, rows, result) {
		if (!err) {
			calculateCurrentStats(STAGE);
		} else {
			logError("Delete items in postgresql STAGE failed: " + err);
		}
	});

	query3("DELETE FROM visits WHERE expires_at < $1", [time], function(err, rows, result) {
		if (!err) {
			calculateCurrentStats(DEVELOPMENT);
		} else {
			logError("Delete items in postgresql DEVELOPMENT failed: " + err);
		}
	});
}

function calculateCurrentStats(env) {
	if (env === PRODUCTION) {
		query("SELECT club_id, round(avg(age),2) age, (count(CASE WHEN is_male THEN 1 ELSE null END) * 100) / count(*) male FROM visits GROUP BY club_id", function(err, rows, result) {
			if (!err) {
				if (rows.length > 0) {
					rows.forEach(function(item) {
						updateClubPG(PRODUCTION, item.club_id, item.age, item.male);
					});
				}
			} else {
				logError("Selection of current stats from postgresql PRODUCTION failed: " + err);
			}
		});
	} else if (env === STAGE) {
		query2("SELECT club_id, round(avg(age),2) age, (count(CASE WHEN is_male THEN 1 ELSE null END) * 100) / count(*) male FROM visits GROUP BY club_id", function(err, rows, result) {
			if (!err) {
				if (rows.length > 0) {
					rows.forEach(function(item) {
						updateClubPG(STAGE, item.club_id, item.age, item.male);
					});
				}
			} else {
				logError("Selection of current stats from postgresql STAGE failed: " + err);
			}
		});
	} else if (env === DEVELOPMENT) {
		query3("SELECT club_id, round(avg(age),2) age, (count(CASE WHEN is_male THEN 1 ELSE null END) * 100) / count(*) male FROM visits GROUP BY club_id", function(err, rows, result) {
			if (!err) {
				if (rows.length > 0) {
					rows.forEach(function(item) {
						updateClubPG(DEVELOPMENT, item.club_id, item.age, item.male);
					});
				}
			} else {
				logError("Selection of current stats from postgresql DEVELOPMENT failed: " + err);
			}
		});
	}
}

function updateClubPG(env, id, age, male) {
	if (env === PRODUCTION) {
		query("UPDATE clubs SET ratio_male = $1, average_age = $2 WHERE id = $3", [male, age, id], function(err, rows, result) {
			if (!err) {
				updateApiProduction(id, age, male);
			} else {
				logError("Current stats update from postgresql PRODUCTION failed: " + err);
			}
		});
	} else if (env === STAGE) {
		query2("UPDATE clubs SET ratio_male = $1, average_age = $2 WHERE id = $3", [male, age, id], function(err, rows, result) {
			if (!err) {
				updateApiStage(id, age, male);
			} else {
				logError("Current stats update from postgresql STAGE failed: " + err);
			}
		});
	} else if (env === DEVELOPMENT) {
		query3("UPDATE clubs SET ratio_male = $1, average_age = $2 WHERE id = $3", [male, age, id], function(err, rows, result) {
			if (!err) {
				updateApiDevelopment(id, age, male);
			} else {
				logError("Current stats update from postgresql DEVELOPMENT failed: " + err);
			}
		});
	}
}

// update item api's
var updateApiProduction = limit(function(id, age, male) {
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
				logError("Update item to dynamodb PRODUCTION failed: " + err);
			}
	});
}).to(5).per(1000);

var updateApiStage = limit(function(id, age, male) {
	dynamodb2.updateItem({
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
				logError("Update item to dynamodb STAGE failed: " + err);
			}
	});
}).to(5).per(1000);

var updateApiDevelopment = limit(function(id, age, male) {
	dynamodb3.updateItem({
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
				logError("Update item to dynamodb DEVELOPMENT failed: " + err);
			}
	});
}).to(5).per(1000);

// main execution
deleteExpiredVisits(new Date(new Date().setDate(new Date().getDate() - 7)).getTime());
