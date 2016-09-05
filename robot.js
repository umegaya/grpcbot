var Fiber = require('fibers');
var _eval = require('eval');
var fs = require('fs');
var URL = require('url');
var crypto = require('crypto');
var Promise = require('any-promise');
var grpc = require('grpc');

module.exports = Robot;

function Robot(script, options) {
	var code = 'module.exports = function (client) {' + 
		'try {\n' + 
			'for (var __count__ = 0; __count__ < ' + (options.loop || 1) + '; __count__++) {\n' + 
				fs.readFileSync(script, { encoding: 'UTF-8' }) + '\n' + 
			'}\n' +
			'client.options.resolve(client, null);\n' + 
		'} catch (e) { client.options.resolve(client, e); throw e; };\n' + 
		'client.finished = true;' +  
	'};';
	//console.log("code = " + code);
	this.id = Robot.idseed++;
	if (!options.API) {
		throw "options.API is required";
	}
	if (!Robot.services[options.API]) {
		Robot.services[options.API] = grpc.load(options.API);
		if (!Robot.services[options.API]) {
			throw "invalid protofile:" + options.API;
		}
	}
	var credential;
	if (options.rootcert) {
		var crt = fs.readFileSync(options.rootcert);
		credential = grpc.credentials.createSsl(crt);//, key, chain);
	} else {
		credential = grpc.createInsecure();
	}
	this.api = new Robot.services[options.API][options.packageName][options.serviceName](options.address, credential);
	this.fiber = Fiber(_eval(code));
	this.options = options;
	this.options.resolve = this.options.resolve || function (cl, err) {
		if (!err) {
			cl.log("running success");
		} else {
			cl.log("robot running error:" + err.stack);
		}
	}
	this.finished = false;
	this.userdata = options.userdataFactory ? options.userdataFactory() : {};
}

Robot.idseed = 1;
Robot.services = {}
Robot.toquery = function(obj) {
	return serialize(obj);
}
Robot.runner = function (script, options) {
	for (var i = 0; i < options.spawnCount; i++) {
		(new Robot(script, options)).run();
	}
}


Robot.prototype.log = function () {
	var params = ""
	if (arguments.length <= 0) {
		return;
	}
	if (arguments.length > 1) {
		var ps = Array.prototype.slice.call(arguments, 1);
		params = " " + JSON.stringify(ps);
	}
	console.log("rb" + this.id + ":" + arguments[0] + params);
}

Robot.prototype.randomBytes = function (len) {
	return crypto.randomBytes(len);
}

Robot.prototype.hash = function (type, fmt, payload) {
	var hash = crypto.createHash(type);
    hash.update(payload);
    return hash.digest(fmt);
}

Robot.prototype.assert = function (cond, msg) {
	if (!cond) {
		self.fiber.throwInto(new Error(msg));
	}
}

Robot.prototype.now = function () {
	return (new Date()).getTime();
}

Robot.prototype.run = function (arg) {
	try {
		this.fiber.run(arg || this);
	} catch (e) {
		console.warn("run robot error:" + e);
	}
}

Robot.prototype.call = function (method, req) {
	var self = this;
	var promise = new Promise(function (resolve, reject) {
		self.api[method](req, function (err, resp) {
			if (err) { reject(err); }
			else { resolve(resp); }
		});
	});
	promise.then(function (res) {
		self.run(res);
	}, function (e) {
		self.fiber.throwInto(e);
	});
	return Fiber.yield();
}

Robot.prototype.sleep = function (msec) {
	var self = this;
	setTimeout(function () {
		self.run();
	}, msec);
	Fiber.yield();
}
