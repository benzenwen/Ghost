// # Joyent Manta Object Store module
// An alternate module for storing images, using a cloud object storage system

var _       = require('lodash'),
    express = require('express'),
    fs      = require('fs-extra'),
    nodefn  = require('when/node/function'),
    nodecb  = require('when/callbacks'),
    path    = require('path'),
    when    = require('when'),
    errors  = require('../errorHandling'),
    config  = require('../config'),
    baseStore   = require('./base'),
    manta   = require('manta'),
    bunyan  = require('bunyan'),
//    mantaLog = bunyan.createLogger({name: "manta"}),

    mantaObjectStore;

mantaObjectStore = _.extend(baseStore, {
    'mantaClient': undefined,
    'createMantaClient': function () {
	// FIXME This should be created at an initialization step on get_storage in parent.
	// createBinClient requires bunyan logger.  createBinClient automatically uses ssh-agent as needed.
	var self = this,
	    url  = process.env.MANTA_URL || 'http://localhost:8080',
	    user = process.env.MANTA_USER || 'admin',
	    mantaLog = bunyan.createLogger({name: 'manta'});
	    opts = {
		connectTimeout: 1000,
		retry: false,
		rejectUnauthorized: (process.env.MANTA_TLS_INSECURE ?
				     false : true),
		url: url,
		user: user,
		log: mantaLog
	    };

	self.mantaClient = manta.createBinClient(opts); 

	return;
    },

    // ### Save
    // Saves the image to storage (the file system)
    // - image is the express image object
    // - returns a promise which ultimately returns the full url to the uploaded image
    'save': function (image) {

	// To match the fn( .. callback, errorback) protocol required by nodecb
	var putWrapper = function (mantapath, inputstream, opts, cb, eb) {
	    inputstream.on('end', function () {
		cb('manta stream done.');
	    });
	    
	    mantaClient.put(mantapath, inputstream, opts, eb);
	}
	    
        var saved = when.defer(),
            targetDir = this.getTargetDir(config().paths.manta.rootDir),
            targetFilename;
        this.getUniqueFileName(this, image, targetDir).then(function (filename) {
            targetFilename = filename; 
	}).then(this.createMantaClient) // How expensive is this, need memoization?
	  .then(function () {
	      var opts = {
		  headers: {
		      'access-control-allow-origin': '*',
		      'access-control-allow-methods': 'GET'
		  },
		  'mkdirs': true
	      };
	      var imgstrm = fs.createReadStream(image.path),
	      mantapath = '~~' + targetFilename;
	      
	      nodecb.call(putWrapper, mantapath, imgstrm, opts); 

	      return;
          }).then(function () {
            return nodefn.call(fs.unlink, image.path).otherwise(errors.logError);
        }).then(function () {
            // The src for the image must be in URI format, not a file system path, which in Windows uses \
            // For local file system storage can use relative path so add a slash
            var fullUrl = process.env.MANTA_URL + '/' + process.env.MANTA_USER + targetFilename;
            return saved.resolve(fullUrl);
        }).otherwise(function (e) {
            errors.logError(e);
            return saved.reject(e);
        });

        return saved.promise;
    },

    'exists': function (filename) {
        // fs.exists does not play nicely with nodefn because the callback doesn't have an error argument
        var done = when.defer();

	// FIXME need to test if it exists.!!!
	done.resolve(function() {
	    return false;
	});

        // fs.exists(filename, function (exists) {
        //     done.resolve(exists);
        // });

        return done.promise;
    },

    // middleware for serving the files
    'serve': function () {
        var ONE_HOUR_MS = 60 * 60 * 1000,
            ONE_YEAR_MS = 365 * 24 * ONE_HOUR_MS;

        // For some reason send divides the max age number by 1000
        return express['static'](config().paths.imagesPath, {maxAge: ONE_YEAR_MS});
    }
});



module.exports = mantaObjectStore;
