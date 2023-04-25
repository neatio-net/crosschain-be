const express = require('express'),
    app = express(),
    mysql = require('mysql2'),
    cors = require('cors'),
    bodyParser = require('body-parser'),
    neatio = require('./neatio'),
    bsc = require('./bsc');

const logger = require('./logger').child({component: "processing"})
logger.info('Neatio bridge started')
console.log('Neatio bridge started')

db = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USERNAME,
    password: process.env.DB_PASS,
    database: process.env.DB_NAME,
    timezone: 'UTC'
});

let bscNonce

async function handleNeatToBscSwap(swap, conP, logger) {
    if (!swap.neat_tx) {
        logger.trace("No Neatio tx")
        return false
    }
    const neatioTx = await neatio.getTransaction(swap.neat_tx)
    if (!neatioTx) {
        logger.trace("Neatio tx doesn't exist")
        return false
    }
    if (!await neatio.isValidSendTx(neatioTx, swap.address, swap.amount, swap.time)) {
        // not valid
        logger.info("Neatio tx is invalid")
        await conP.execute("UPDATE `swaps` SET `status` = 'Fail', `fail_reason` = 'Not Valid' WHERE `uuid` = ?", [swap.uuid])
        return true
    }
    if (!await neatio.isNewTx(swap.neat_tx)) {
        // not new
        logger.info("Neatio tx already used")
        await conP.execute("UPDATE `swaps` SET `status` = 'Fail', `fail_reason` = 'Not Valid' WHERE `uuid` = ?", [swap.uuid])
        return true
    }
    if (!await neatio.isTxConfirmed(neatioTx)) {
        // waiting to be confirmed
        logger.debug("Neatio tx is not confirmed")
        await conP.execute("UPDATE `swaps` SET `mined` = '0' WHERE `uuid` = ?", [swap.uuid])
        return true
    }
    if (!await neatio.isTxActual(neatioTx, swap.time)) {
        // not valid (not actual)
        logger.info("Neatio tx is not actual")
        await conP.execute("UPDATE `swaps` SET `status` = 'Fail', `fail_reason` = 'Not Valid' WHERE `uuid` = ?", [swap.uuid])
        return true
    }
    const estimateRes = await bsc.estimateMint(swap.address, swap.amount);
    if (!estimateRes) {
        logger.info("Unable to estimate mint bsc coins, there will be retry")
        return false
    }
    // confirmed
    const [data] = await conP.execute("INSERT INTO `used_txs` (`blockchain`,`tx_hash`) VALUES ('neatio', ?);", [swap.neat_tx]);
    if (!data.insertId) {
        logger.error("Unable to insert used neatio tx")
        return true
    }
    const customNonce = bscNonce
    let {
        hash,
        nonce,
        gasPrice,
        gasLimit,
        fees
    } = await bsc.mint(estimateRes.contract, swap.address, estimateRes.amount, estimateRes.fees, customNonce);
    if (!hash) {
        logger.error("Unable to mint bsc coins")
        await conP.execute("UPDATE `swaps` SET `status` = 'Fail' ,`mined` = '1' ,`fail_reason` = 'Unknown' WHERE `uuid` = ?", [swap.uuid])
        return true
    }
    bscNonce++
    logger.info(`Swap completed, bsc tx hash: ${hash}, fees: ${fees}, nonce: ${nonce}, gasPrice: ${gasPrice}, gasLimit: ${gasLimit}`)
    await conP.execute("UPDATE `swaps` SET `status` = 'Success' ,`mined` = '1' ,`bsc_tx` = ? ,`fees` = ? WHERE `uuid` = ?", [hash, fees, swap.uuid])
    return true
}

async function handleBscToNeatSwap(swap, conP, logger) {
    if (!swap.bsc_tx) {
        logger.trace("No BSC tx")
        return false
    }
    const {valid, retry, txReceipt} = await bsc.validateBurnTx(null, swap.bsc_tx, swap.address, swap.amount, swap.time)
    if (!valid) {
        if (retry) {
            logger.info("BSC tx is invalid, there will be retry")
            return false
        }
        // not valid
        logger.info("BSC tx is invalid")
        await conP.execute("UPDATE `swaps` SET `status` = 'Fail', `mined` = '2', `fail_reason` = 'Not Valid' WHERE `uuid` = ?", [swap.uuid])
        return true
    }
    if (!await bsc.isNewTx(swap.bsc_tx)) {
        // not new
        logger.info("BSC tx already used")
        await conP.execute("UPDATE `swaps` SET `status` = 'Fail', `mined` = '2', `fail_reason` = 'Not Valid' WHERE `uuid` = ?", [swap.uuid])
        return true
    }
    if (!await bsc.isTxConfirmed(txReceipt)) {
        // waiting to be confirmed
        logger.debug("BSC tx is not confirmed")
        await conP.execute("UPDATE `swaps` SET `mined` = '0' WHERE `uuid` = ?", [swap.uuid])
        return true
    }
    // confirmed
    const [data] = await conP.execute("INSERT INTO `used_txs`(`blockchain`,`tx_hash`) VALUES ('bsc',?);", [swap.bsc_tx]);
    if (!data.insertId) {
        logger.error("Unable to insert used BSC tx")
        return true
    }
    let {
        hash,
        fees,
        errorMessage
    } = await neatio.send(swap.address, swap.amount, true);
    if (!hash) {
        const reason = errorMessage ? errorMessage : 'Unknown'
        logger.error(`Unable to send neatio tx: ${reason}`)
        await conP.execute("UPDATE `swaps` SET `status` = 'Fail' ,`mined` = '1' ,`fail_reason` = ? WHERE `uuid` = ?", [reason, swap.uuid])
        return true
    }
    logger.info(`Swap completed, neatio tx hash: ${hash}`)
    await conP.execute("UPDATE `swaps` SET `status` = 'Success' ,`mined` = '1' ,`neat_tx` = ? , `fees` = ? WHERE `uuid` = ?", [hash, fees, swap.uuid])
    return true
}

async function handleSwap(swap, conP, logger) {
    let handled
    if (swap.type === 0) {
        handled = await handleNeatToBscSwap(swap, conP, logger)
    }
    if (swap.type === 1) {
        handled = await handleBscToNeatSwap(swap, conP, logger)
    }
    if (handled) {
        return
    }
    let date = new Date(swap.time);
    date.setDate(date.getDate() + 1);
    if (date < Date.now()) {
        logger.info("Swap is outdated")
        await conP.execute("UPDATE `swaps` SET `status` = 'Fail', `fail_reason` = 'Time' WHERE `uuid` = ?", [swap.uuid])
        return
    }
    logger.trace("Swap skipped")
}

async function checkSwaps() {
    logger.trace("Starting to check swaps")
    let conP = db.promise();
    let sql = "SELECT * FROM `swaps` WHERE `status` = 'Pending';";
    let data
    try {
        [data] = await conP.execute(sql);
    } catch (error) {
        logger.error(`Failed to load pending swaps: ${error}`);
        return
    }
    if (!data.length) {
        return
    }
    logger.trace(`Starting to handle pending swaps, cnt: ${data.length}`)
    for (swap of data) {
        const swapLogger = logger.child({swapId: swap.uuid})
        try {
            await handleSwap(swap, conP, swapLogger)
        } catch (error) {
            swapLogger.error(`Failed to handle swap: ${error}`);
        }
    }
}

async function checkRefunds() {
    logger.trace("Starting to check refunds")
    let conP = db.promise();
    let sql = "SELECT `id`, `address`, `amount` FROM `pending_refunds`";
    let data
    try {
        [data] = await conP.execute(sql);
    } catch (error) {
        logger.error(`Failed to load pending swaps: ${error}`);
        return
    }
    if (!data.length) {
        return
    }
    logger.trace(`Starting to handle pending refunds, cnt: ${data.length}`)
    for (refund of data) {
        const refundLogger = logger.child({refundId: refund.id})
        try {
            await handleRefund(refund, conP, refundLogger)
        } catch (error) {
            refundLogger.error(`Failed to handle refund: ${error}`);
        }
    }
}

async function handleRefund(refund, conP, logger) {
    logger.info(`Starting to handle refund, address: ${refund.address}, amount: ${refund.amount}`)
    await conP.execute("DELETE FROM `pending_refunds` WHERE `id` = ?", [refund.id])
    logger.info('Deleted pending refund')
    let {hash, errorMessage} = await neatio.send(refund.address, refund.amount, false);
    if (!hash) {
        const reason = errorMessage ? errorMessage : 'Unknown'
        logger.error(`Unable to send neatio tx: ${reason}`)
        return
    }
    logger.info(`Sent neatio tx ${hash}`)
    await conP.execute("INSERT INTO `refunds`(`neat_tx`) VALUES (?);", [hash]);
    logger.info(`Refund completed`)
}

async function loopCheckSwaps() {
    await checkRefunds();
    await checkSwaps();
    setTimeout(loopCheckSwaps, parseInt(process.env.CHECKING_DELAY));
}

const swaps = require('./routes/swaps');
app.use(cors())
app.use(bodyParser.json());
app.use('/swaps', swaps);

async function start() {
    await neatio.initNonce()

    bscNonce = await bsc.getNonce()
    logger.info(`BSC nonce initialized: ${bscNonce}`)
    console.log(`BSC nonce initialized: ${bscNonce}`)

    loopCheckSwaps();
    bsc.loopTokenSupplyRefreshing();
    const port = 8000;
    app.listen(port, () => logger.info(`Server started, listening on port: ${port}`));
}

start()
