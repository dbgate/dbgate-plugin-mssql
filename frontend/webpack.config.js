var webpack = require("webpack");
var path = require("path");

var config = {
  context: __dirname + "/src",

  entry: {
    app: "./index.js",
  },
  target: "web",
  output: {
    path: path.resolve(__dirname, "..", "lib"),
    filename: "frontend.js",
    libraryTarget: "var",
    library: 'plugin',
  },

  // optimization: {
  //   minimize: false,
  // },
};

module.exports = config;
