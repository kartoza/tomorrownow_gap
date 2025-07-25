const path = require("path");
const BundleTracker = require('webpack-bundle-tracker');
const MiniCssExtractPlugin = require('mini-css-extract-plugin');
const { CleanWebpackPlugin } = require('clean-webpack-plugin'); // require clean-webpack-plugin
const ReactRefreshWebpackPlugin = require('@pmmmwh/react-refresh-webpack-plugin');
const TerserPlugin = require('terser-webpack-plugin');
const webpack = require("webpack");
const { Sign } = require("crypto");

const mode = process.env.npm_lifecycle_script;
const isDev = (mode.includes('dev'));
const isServe = (mode.includes('serve'));
const filename = isDev ? "[name]" : "[name].[fullhash]";
const statsFilename = isDev ? './webpack-stats.dev.json' : './webpack-stats.prod.json';
const minimized = !isDev;
// Read ENV (default: true)
let parallelValue = process.env.WEBPACK_PARALLEL;

let parallel;
// If explicitly "true"/"false", convert to boolean
if (parallelValue === 'true') {
  parallel = true; // auto = CPU cores
} else if (parallelValue === 'false') {
  parallel = false; // single-threaded
} else if (!isNaN(Number(parallelValue))) {
  parallel = Number(parallelValue); // e.g. "2" => 2 workers
} else {
  parallel = 2;
}


let conf = {
    entry: {
        App: './src/index.tsx',
        Swagger: './src/Swagger.tsx',
    },
    output: {
        path: path.resolve(__dirname, "./bundles/frontend"),
        filename: filename + '.js'
    },
    module: {
        rules: [
            {
                test: /\.tsx?$/,
                exclude: /node_modules/,
                use: [{ loader: 'ts-loader' }],
            },
            {
                test: /\.s[ac]ss$/i,
                use: [
                    MiniCssExtractPlugin.loader,
                    "css-loader",
                    "sass-loader"
                ],
            },
            {
                test: /\.css$/i,
                use: [
                    // Translates CSS into CommonJS
                    MiniCssExtractPlugin.loader, "css-loader",
                ],
            },
        ],
    },
    optimization: {
        minimize: minimized,
        minimizer: [
            new TerserPlugin({
                parallel: parallel // limit to n workers
            })
        ],
        splitChunks: {
            cacheGroups: {
                styles: {
                    name: "styles",
                    type: "css/mini-extract",
                    chunks: "all",
                    enforce: true,
                },
            },
        },
    },
    plugins: [
        new webpack.DefinePlugin({
          'process.env.NODE_DEBUG': JSON.stringify(process.env.NODE_DEBUG),
        }),
        new CleanWebpackPlugin(),
        new BundleTracker({ filename: statsFilename }),
        new MiniCssExtractPlugin({
            filename: filename + '.css',
            chunkFilename: filename + '.css',
            ignoreOrder: true,
        }),
    ],
    resolve: {
        modules: ['node_modules'],
        alias: {
            '@': path.resolve(__dirname, 'src'),
            // Specific folder aliases
            '@app': path.resolve(__dirname, 'src/app'),
            '@components': path.resolve(__dirname, 'src/components'),
            '@features': path.resolve(__dirname, 'src/features'),
            '@layouts': path.resolve(__dirname, 'src/layouts'),
            '@pages': path.resolve(__dirname, 'src/pages'),
            '@utils': path.resolve(__dirname, 'src/utils'),
            '@hooks': path.resolve(__dirname, 'src/hooks'),
            '@services': path.resolve(__dirname, 'src/services'),
            '@assets': path.resolve(__dirname, 'src/assets'),
            '@styles': path.resolve(__dirname, 'src/styles'),
            '@context': path.resolve(__dirname, 'src/context'),
        },
        extensions: [".ts", ".tsx", ".js", ".css", ".scss"],
        fallback: {
            fs: false,
        }
    },
    watchOptions: {
        ignored: ['node_modules', './**/*.py'],
        aggregateTimeout: 300,
        poll: 1000
    }
};
if (isServe) {
    if (isDev) {
        conf['output'] = {
            path: path.resolve(__dirname, "./bundles/frontend"),
            filename: filename + '.js',
            publicPath: 'http://localhost:9000/static/',
        }
    }
    conf['devServer'] = {
        hot: true,
        port: 9000,
        headers: {
            'Access-Control-Allow-Origin': '*'
        },
        devMiddleware: {
            writeToDisk: true,
        },
        allowedHosts: 'all',
        compress: true,
    }
    conf['devtool'] = 'inline-source-map',
    conf['output'] = {
        path: path.resolve(__dirname, "./bundles/frontend"),
        filename: filename + '.js',
        publicPath: 'http://localhost:9000/static/',
    }
    conf['plugins'].push(
        new ReactRefreshWebpackPlugin()
    )
} else if (isDev) {
    conf['output'] = {
        path: path.resolve(__dirname, "./bundles/frontend"),
        filename: filename + '.js'
    }
    conf['devServer'] = {
        hot: true,
        port: 9000,
        writeToDisk: true,
        headers: {
            "Access-Control-Allow-Origin": "*",
        }
    }
}
module.exports = conf;
