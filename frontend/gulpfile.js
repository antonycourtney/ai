var gulp = require('gulp'); 

//For cleaning out the build dir
var clean = require('gulp-clean');

// Allow tasks to be run in serial as well as parallel (especially clean before other tasks)
var runSequence = require('run-sequence');

//For processing react and other files into minimized files
var react = require('gulp-react');
var uglify = require('gulp-uglify');
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

var webpack = require("webpack");
var webpackConfig = require("./webpack.config.js");

// Delete everything inside the build directory
gulp.task('clean', function() {
  return gulp.src(['build/*'], {read: false}).pipe(clean());
});

// Copy all jquery files to dist:
gulp.task('build_jquery', function() {
    gutil.log("copy jquery/dist from node_modules to build");

    return gulp.src('../node_modules/jquery/dist/**/*')
        .pipe(gulp.dest('build/js'));
});

// Copy all bootstrap files from node_modules/bootstrap/dist:
gulp.task('build_bootstrap', function() {
    gutil.log("copy bootstrap/dist from node_modules to build");

    return gulp.src('../node_modules/bootstrap/dist/**/*')
        .pipe(gulp.dest('build'));
});

// Copy plottable files (from bower_components):
gulp.task('plottable_css', function() {
    return gulp.src('../bower_components/plottable/*.css')
        .pipe(gulp.dest('build/css'));
});

gulp.task('plottable_js', function() {
    return gulp.src('../bower_components/plottable/*.js')
        .pipe(gulp.dest('build/js'));
});

gulp.task('build_plottable', ['plottable_css', 'plottable_js']);

// build 3rd dependent libs:
gulp.task('build_deplibs',['build_jquery','build_bootstrap','build_plottable'], function() {
});

gulp.task('build_javascript', function() {

    gutil.log("transforming jsx to build");

    // Take every JS file in ./client/js
    return gulp.src('client/js/**/*.js')
        // .pipe(debug())
        // Turn their React JSX syntax into regular javascript
        .pipe(react())
        // Output each one of those --> ./build/js/ directory
        .pipe(gulp.dest('build/js/'))
        // Then take each of those and minimize
        .pipe(uglify())
        // Add .min.js to the end of each optimized file
        .pipe(rename({suffix: '.min'}))
        // Then output each optimized .min.js file --> ./build/js/ directory
        .pipe(gulp.dest('build/js/'));
});
/*
var scriptPageDeps = [];

function browserifyPageScript(scriptBaseName) {
    scriptPageDeps.push('browserify_' + scriptBaseName);
    return gulp.task('browserify_' + scriptBaseName, ['build_deplibs','build_javascript'], function() {
        gutil.log("running browserify for " + scriptBaseName + ".js");

        return gulp.src('build/js/' + scriptBaseName + '.js')
            .pipe(browserify({
                transform: ['envify']
            }))
            .pipe(rename(scriptBaseName + '.build.js'))
            .pipe(gulp.dest('build/js/'))
            .pipe(uglify())
            .pipe(rename({suffix: '.min'}))
            .pipe(gulp.dest('build/js/'));
    });
}

browserifyPageScript('home');
browserifyPageScript('correspondentPage');
browserifyPageScript('allCorrespondents');
browserifyPageScript('correspondentRankings');

gulp.task('browserify', 
     scriptPageDeps,
     function() { });
*/

gulp.task('watch', function() {

    runSequence('clean', ['build_jquery', 'build_bootstrap']);

    var watching = false;
    gulp.start('browserify', function() {

        // Protect against this function being called twice. (Bug?)
        if (!watching) {
            watching = true;

            // Watch for changes in client javascript code and run the 'browserify' task
            gulp.watch(['client/**/*.js', 'test/**/*.test.js'], ['browserify']);

            // Restart node if anything in . changes
            nodemon({script: 'server.js', watch: '.'});
        }
    });
});

// Build and watch cycle (another option for development)
// Advantage: No server required, can run app from filesystem
// Disadvantage: Requests are not blocked until bundle is available,
//               can serve an old app on refresh
gulp.task("build-dev", ["webpack:build-dev"], function() {
    gulp.watch(["public/**/*","./*"], ["webpack:build-dev"]);
});


gulp.task("webpack:build", ["build_deplibs"], function(callback) {
    // modify some webpack config options
    var myConfig = Object.create(webpackConfig);
    if (myConfig.plugins) {
        myConfig.plugins = myConfig.plugins.concat(
            new webpack.DefinePlugin({
                "process.env": {
                    // This has effect on the react lib size
                    "NODE_ENV": JSON.stringify("production")
                }
            }),
            new webpack.optimize.DedupePlugin(),
            new webpack.optimize.UglifyJsPlugin()
        );
    }
    // run webpack
    webpack(myConfig, function(err, stats) {
        if(err) throw new gutil.PluginError("webpack:build", err);
        gutil.log("[webpack:build]", stats.toString({
            colors: true
        }));
        callback();
    });
});

// Production build
gulp.task("build_js", ["webpack:build"]);

// modify some webpack config options
var myDevConfig = Object.create(webpackConfig);
myDevConfig.devtool = "sourcemap";
myDevConfig.debug = true;

// create a single instance of the compiler to allow caching
var devCompiler = webpack(myDevConfig);

gulp.task("webpack:build-dev", ["build_deplibs"], function(callback) {
    // run webpack
    devCompiler.run(function(err, stats) {
        if(err) throw new gutil.PluginError("webpack:build-dev", err);
        gutil.log("[webpack:build-dev]", stats.toString({
            colors: true
        }));
        callback();
    });
});


gulp.task("default", ["build_js"] );