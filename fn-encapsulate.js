'use strict';

var crypto = require('crypto');
var fs = require('fs');
var path = require('path');


var Module = module.constructor;

module.exports = (functionFilename, expectedFunctionName) => {

	function generateFakeFilename() {
		let absoluteFilename = path.resolve(functionFilename);
		let hash = crypto.createHmac('sha256', 'encap').update(absoluteFilename).digest('hex');
		let shortHash = hash.substr(hash.length - 16, hash.length - 1);
		return 'fn_encapsulator_' + shortHash;
	}

	function createFakeModule(fakeFilename, functionName, functionCode) {
		let moduleCode = "module.exports." + functionName + " = (" + functionCode + ");\n";
		let m = new Module(fakeFilename, module.parent);
		m.paths = Module._nodeModulePaths(path.dirname('.'));
		m._compile(moduleCode, fakeFilename);
		return m;
	}

	function validateModule(m) {
		let exportedFunctions = Object.keys(m.exports);
		if (!exportedFunctions || exportedFunctions.length !== 1) {
			throw new Error("Expected exactly one function in function file.");
		}
	}

	let fakeFilename = generateFakeFilename();
	let functionCode = fs.readFileSync(functionFilename, 'utf8');
	let m = createFakeModule(fakeFilename, expectedFunctionName, functionCode);
	validateModule(m);
	return m.exports;
};
