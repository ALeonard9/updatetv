const mysql = require('mysql');
const request = require('request');
const async = require('async');
const winston = require('winston');
const chalk = require('chalk');
const mysql_config = require('./config/db');

var AWS = require("aws-sdk");

var sns = new AWS.SNS();

winston.level =  process.env.winston || 'info'

var today = new Date;

winston.info(chalk.yellow('Update Tv status function began at ' + today));

function updateTV(){
  let p1 = new Promise(function(resolve, reject){
    var connection = mysql.createConnection(mysql_config);
    connection.connect(function(err) {
      if (!err) {
          connection.query("SELECT id, title, imdb, status FROM tv",
              function(err, results) {
                  if (!err) {
                      winston.log('info', chalk.yellow('Total number of shows: ' + results.length));
                      connection.end();
                      resolve(results);
                  } else {
                      reject(err);
                      winston.log('error', chalk.red('Query error: ' + err));
                  }
              });
      } else {
        error_handler('Database connection error: ' + err.message);
      }
  });   
  })
  
  p1.then(function(results){
    var i = 0;
    async.each(results, function(result, callback){
        winston.log('verbose', chalk.cyan(result.title, " ", result.status, " ", result.id, " ", result.imdb));
        setTimeout(function(){
            var url = null;
            url = 'http://api.tvmaze.com/lookup/shows?imdb=' + result.imdb
            winston.log('debug', chalk.cyan(url));
            request(url, function (error, response, body) {
                parsedBody = JSON.parse(body);
                if( result.status != parsedBody.status){
                    var connection = mysql.createConnection(mysql_config);
                    connection.connect(function(err) {
                        if (!err) {
                            connection.query('UPDATE tv SET status= ? WHERE  id = ?', [parsedBody.status, result.id],
                            function(err, results) {
                                if (!err) {
                                    connection.end();
                                    updated_text = result.title + ' has been updated from ' + result.status + ' to ' + parsedBody.status + '.'
                                    winston.log('info', chalk.green(updated_text));
                                    var params = {
                                        Message: updated_text, 
                                        Subject: "UpdateTV Lambda Notice",
                                        TopicArn: "arn:aws:sns:us-east-1:626550523583:NotifyMe"
                                    };
                                    sns.publish(params,function(err, data) {
                                        if(err) {
                                            winston.log('error', chalk.red('SNS error: ' + err));
                                        }
                                    });
                                } else {
                                    error_handler('Query error: ' + err);
                                }
                            });
                        } else {
                            error_handler('Database connection error: ' + err.message);
                        }
                    })    
                }
          })
        }, 1000 * i);
        i++;        
    },function(err){
        error_handler('Async error: ' + err);        
    })

    }
  )
}

function error_handler (message) {
    winston.log('error', chalk.red(message));    
    var params = {
        Message: message, 
        Subject: "UpdateTV Lambda ERROR Notice",
        TopicArn: "arn:aws:sns:us-east-1:626550523583:NotifyMe"
    };
    sns.publish(params,function(err, data) {
        if(err) {
            winston.log('error', chalk.red('SNS error: ' + err));
        }
    });
}

exports.handler = (event, context, callback) => {
  var result = updateTV();
  callback(null, result)
};

