const mysql = require('mysql');
const request = require('request');
const async = require('async');
const winston = require('winston');
const chalk = require('chalk');
const mysql_config = require('./config/db');

var environment =  process.env.environment || 'local'

if (environment == 'aws'){
    var AWS = require('aws-sdk');
    var sns = new AWS.SNS();
}

var today = new Date();
var now = Math.floor(new Date() / 1000);
var comparedate = now - 172800;
// Convert to milliseconds
var comparedate_string = new Date(comparedate *1000);

winston.info(chalk.yellow('Update Tv status function began at ' + today));

winston.level =  process.env.winston || 'info';

function updateTV(){
    let p1 = new Promise((resolve, reject) => { 
        url = 'http://api.tvmaze.com/updates/shows'
        updatedshows = [];
        winston.log('debug', chalk.cyan(url));
        request(url, function (err, response, body) {
            if (err) {
                reject('request failed')
            } else {
                parsedBody = JSON.parse(body);
                for(var key in parsedBody){
                    if (parsedBody[key] > comparedate) {
                        updatedshows.push(key)
                    }
                }
                resolve(updatedshows)
            }
        })
    });
    
    let p2 = new Promise(function(resolve, reject){
        var connection = mysql.createConnection(mysql_config);
        connection.connect(function(err) {
        if (!err) {
            connection.query("SELECT * FROM tv",
                function(err, results) {
                    if (!err) {
                        winston.log('info', chalk.yellow('Total number of shows: ' + results.length));
                        connection.end();
                        resolve(results);
                    } else {
                        reject(err);
                        error_handler('Query error: ' + err);
                    }
                });
        } else {
            error_handler('Database connection error: ' + err.message);
        }
    });   
    }) 

    Promise.all([p1, p2]).then(values => { 
        var i = 0;
        async.each(values[1], function(result, callback){
            winston.log('verbose', chalk.cyan(result.title, " ", result.status, " ", result.id, " ", result.imdb, ' ', result.tvmaze));
            var tvmazeint = result.tvmaze.toString();
            if (values[0].includes(tvmazeint) ){
                winston.info(chalk.yellow(result.title, ' has been updated after ', comparedate_string));                
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
                                            if (environment == 'aws'){   
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
                                            }    
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
            }
        },function(err){
            error_handler('Async error: ' + err);        
        })
    }, reason => {
        error_handler(reason)
    });
} 

  function error_handler (message) {
    winston.log('error', chalk.red(message));
    if (environment == 'aws'){
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
}

if (environment == 'local'){
    updateTV();
}

exports.handler = (event, context, callback) => {
    var result = updateTV();
    callback(null, result)
};