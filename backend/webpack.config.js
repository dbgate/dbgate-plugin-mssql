var webpack = require('webpack');
var path = require('path');

var config = {
  context: __dirname + '/src',

  entry: {
    app: './index.js',
  },
  target: 'node',
  output: {
    path: path.resolve(__dirname, "..", "lib"),
    filename: "backend.js",
    libraryTarget: 'commonjs2',
  },

//   optimization: {
//     minimize: false,
//   },
};

module.exports = config;
