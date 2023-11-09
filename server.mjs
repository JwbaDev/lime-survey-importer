import * as mariadb from 'mariadb';
import express from 'express';
import * as eta from 'eta';
import fileUpload from 'express-fileupload';
import { XMLParser } from 'fast-xml-parser';
import Excel from 'exceljs';

const pool = mariadb.createPool({
	host: '127.0.0.1',
	database: 'limesurveydb',
	user: 'limesurveyuser',
	password: 'LVosvnmd1KF5q',
	port: 3306,
	connectionLimit: 5
});

const app = express();

eta.configure({ views: "./views", cache: true });
app.set("views", "./views");
app.set("view cache", true);
app.set("view engine", "eta");
app.use('/public', express.static('public'));

app.use(fileUpload());

app.get('/', async (req, res) => {
	res.render('upload');
});

class Question {
	constructor(type, gid, qid, title, hasOther) {
		// Limesurvey question types:
		// L - list (radio buttons)
		// S - short free text
		// M - multiple choice
		// F - array of radio buttons
		// : - array of numbers?
		this.type = type;
		this.gid = gid;
		this.qid = qid;
		this.title = title;
		this.hasOther = hasOther;
		this.subquestions = [];
	}
}

class Subquestion {
	constructor(qid, title, scale) {
		this.qid = qid;
		this.title = title;
		// A better name they could've chosen is "axis". Either 0 or 1
		this.scale = scale;
	}
}

function parseLSS(buf) {
	const xmlParser = new XMLParser();
	const content = xmlParser.parse(buf).document;

	let surveyId = null;

	const questions = [];

	for(const row of content.questions.rows.row) {
		surveyId = row.sid; // They are all the same anyways.
		questions.push(new Question(row.type, row.gid, row.qid, row.title, row.other === 'Y'));
	}

	for(const row of content.subquestions.rows.row) {
		const par = questions.find(q => q.qid === row.parent_qid && q.gid == row.gid);
		par.subquestions.push(new Subquestion(row.qid, row.title, row.scale_id));
		
		// const [parentQcode, parentDbcol] = parent;
		// qcode2db[`${parentQcode}_${row.title}`] = `${parentDbcol}${row.title}`;
		// toRemove.add(parentQcode);
	}

	const qcode2db = {
		'id': 'id',
		'submitdate': 'submitdate',
		'lastpage': 'lastpage',
		'startlanguage': 'startlanguage',
		'startdate': 'startdate',
		'datestamp': 'datestamp',
	};

	for(const q of questions) {
		if (q.subquestions.length == 0) {
			qcode2db[q.title] = `${surveyId}X${q.gid}X${q.qid}`;
			if(q.hasOther) {
				qcode2db[q.title+'_other'] = `${surveyId}X${q.gid}X${q.qid}other`;
			}
		} else {
			const scale0 = q.subquestions.filter(sq => sq.scale === 0);
			const scale1 = q.subquestions.filter(sq => sq.scale === 1);
			for (const sq0 of scale0) {
				if (scale1.length === 0) {
					qcode2db[`${q.title}_${sq0.title}`] = `${surveyId}X${q.gid}X${q.qid}${sq0.title}`;
				} else {
					for (const sq1 of scale1) {
						qcode2db[`${q.title}_${sq0.title}_${sq1.title}`] = `${surveyId}X${q.gid}X${q.qid}${sq0.title}_${sq1.title}`;
					}
				}
			}
		}
	}

	return { dbname: `survey_${surveyId}`, qcode2db };
}

function getValues(row) {
	const values = [];
	row.eachCell((cell, _) => {
		// TODO check cell.type
		values.push(cell.value);
	});
	return values;
}

async function parseXLSX(buf) {
	const workbook = new Excel.Workbook();
	await workbook.xlsx.load(buf);
	const sheet = workbook.worksheets[0];
	
	const qcodes = getValues(sheet.getRow(1)); // Header
	const data = [];

	sheet.eachRow((row, rolNum) => {
		if(rolNum == 1) return;
		let values = [];
		for(let i = 0;i < qcodes.length;i ++) {
			values.push(row.getCell(i+1).value);
		}
		data.push(values);
	})

	return { qcodes, data };
}

function makeSQL(dbname, qcodes, qcode2db, data) {
	const orderedFields = qcodes.map(qcode => qcode2db[qcode]);
	const placeHolders = Array(qcodes.length).fill('?');

	return `
		INSERT INTO ${dbname} (${orderedFields.join(',')})
		VALUES (${placeHolders.join(',')})
	`;
}

function reformatDataIds(data) {
	// Randomize the ids (assumed to be the first column)
	const pad = (n, v) => String(v).padStart(n, '0');
	const rnd = Math.floor(Math.random() * 10000);

	for(const row of data) {
		row[0] = Number(`${pad(4, rnd)}${pad(5, row[0])}`);
	}

	return data;
}

app.post('/', async (req, res) => {
	const { dbname, qcode2db } = parseLSS(req.files.lss.data);
	const { qcodes, data } = await parseXLSX(req.files.xlsx.data);

	// console.log(qcodes)
	// console.log(qcode2db)
	// return res.end();
	console.log(`[${new Date()}] Importing ${data.length} rows to ${dbname}...`);

	const excelErrors = [];
	for(const qcode of qcodes) {
		if(!qcode2db.hasOwnProperty(qcode)) {
			excelErrors.push(`Excel header "${qcode}" is not present in this survey`);
		}
	}
	if(excelErrors.length > 0) {
		return res.render('upload', { errors: excelErrors });
	}

	reformatDataIds(data);
	const sql = makeSQL(dbname, qcodes, qcode2db, data);

	try {
		await pool.batch(sql, data);
	} catch(err) {
		console.log(`[${new Date()}] Error ${dbname}: ${err}`);
		return res.render('upload', { errors: [err] })
	}

	console.log(`[${new Date()}] OK ${dbname}`);
	res.render('success', { count: data.length });
});

const port = process.env.PORT || 8081;
app.listen(port, () => {
	console.log(`Example app listening on port ${port}`)
})
