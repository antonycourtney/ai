// webpack.config.js
module.exports = {
    entry: {
        home: "./client/js/home.js",
        correspondentPage: "./client/js/correspondentPage.js",
        owedEmail: "./client/js/owedEmail.js",
        allCorrespondents: "./client/js/allCorrespondents.js",
        correspondentRankings: "./client/js/correspondentRankings.js"
    },
    output: {
        path: "./build/js",
        filename: "[name].bundle.js"
    },
    module: {
        loaders: [
            { test: /\.(js|jsx)$/, loader: "jsx-loader?harmony" },
            { test: /\.css$/, loader: "style-loader!css-loader" },
            { test: /\.(png|jpg)$/, loader: "url-loader?limit=8192"},
            { test: /\.json$/, loader: "json"}
        ]
    },
    resolve: {
        // require('module') instead of require('module.js')
        extensions: ["", ".js", ".jsx", ".json"]
    }
};