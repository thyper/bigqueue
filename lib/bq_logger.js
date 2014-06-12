/**
 * We'll do a singleton logger to made easy create and change configurations
 */
var winston = require("winston")

try {
    winston.remove(winston.transports.Console)
} catch(e) {
}

winston.setLevel = function(level) {
  try {
    winston.remove(winston.transports.Console)
  } catch(e) {}
  winston.add(winston.transports.Console, {timestamp:true, colorize: true, level: level});

}

module.exports = winston; 
