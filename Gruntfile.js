const connect = require('connect');
const serveStatic = require('serve-static');
const fork = require('child_process').fork;
const grunt = require('grunt');
var cloudServer;
var clientServer;

grunt.registerTask('startServers', function startServers() {
  process.env.FH_USE_LOCAL_DB = true;
  const done = this.async();
  cloudServer = fork(__dirname + '/application.js');
  clientServer = connect().use(serveStatic(__dirname)).listen(9002, function() {
    return done();
  });
});

grunt.registerTask('stopServers', function stopServers() {
  if (clientServer) {
    clientServer.close();
  }
  if (cloudServer) {
    cloudServer.kill();
  }
});

module.exports = function(grunt) {
  grunt.initConfig({
    jasmine: {
      all: {
        src: 'main.js',
        options: {
          host: 'http://localhost:9002',
          outfile: 'index.html?url=http://localhost:8001',
          spec: 'spec/*/**Spec.js'
        }
      }
    },
    eslint: {
      target: ['app.js', 'application.js', 'spec/*.js']
    }
  });

  grunt.loadTasks('tasks');
  grunt.loadNpmTasks('grunt-contrib-jasmine');
  grunt.loadNpmTasks('grunt-eslint');
  grunt.registerTask('default', ['eslint', 'startServers', 'jasmine', 'stopServers']);
};



