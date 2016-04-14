var path = require('path');
var crypto = require('crypto');
var fs = require('fs');

var Module = module.constructor;

module.exports = function(functionFilename, expectedFunctionName) {

	function generateFakeFilename() {
		var absoluteFilename = path.resolve(functionFilename);
		var hash = crypto.createHmac('sha256', 'encap').update(absoluteFilename).digest('hex');
		var shortHash = hash.substr(hash.length - 16, hash.length - 1);
		return 'fn_encapsulator_' + shortHash;
	}

	function createFakeModule(fakeFilename, functionName, functionCode) {

		var moduleCode = "module.exports." + functionName + " = (" + functionCode + ");\n";

		var m = new Module(fakeFilename, module.parent);
		m.paths = Module._nodeModulePaths(path.dirname('.'));
		m._compile(moduleCode, fakeFilename);
		return m;
	}

	function validateModule(m) {
		var exportedFunctions = Object.keys(m.exports);

		if (!exportedFunctions || exportedFunctions.length !== 1) {
			throw new Error("Expected exactly one function in function file.");
		}
	}

	var fakeFilename = generateFakeFilename();
	var functionCode = fs.readFileSync(functionFilename, 'utf8');

	var m = createFakeModule(fakeFilename, expectedFunctionName, functionCode);

	validateModule(m);

	return m.exports;
};
