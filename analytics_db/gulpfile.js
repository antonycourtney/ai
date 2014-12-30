var gulp = require('gulp'); 

//For cleaning out the build dir
var clean = require('gulp-clean');

// Allow tasks to be run in serial as well as parallel (especially clean before other tasks)
var runSequence = require('run-sequence');

//For processing react and other files into minimized files
var rename = require('gulp-rename');
var traceur = require('gulp-traceur');
var debug = require('gulp-debug');

//For browserify build
var browserify = require('gulp-browserify');

//Mocha tests
var mochaPhantomJS = require('gulp-mocha-phantomjs');

//For re-running node when server source changes
var nodemon = require('gulp-nodemon');

//Convert all js file jsdocs annotation to markdown
var jsdoc2md = require("jsdoc-to-markdown");
var gutil = require("gulp-util");

var fs = require("fs");

// Delete everything inside the build directory
gulp.task('clean', function() {
  return gulp.src(['build/*'], {read: false}).pipe(clean());
});

// We'll run .js files in the queries subdir through the Traceur compiler,
// which we use for template strings:
gulp.task('build_queries', function() {
    return gulp.src('queries/js/**/*.js')
        .pipe(traceur())
        .pipe(gulp.dest('build/js/'));    
});

gulp.task('default', ['build_queries'] );
