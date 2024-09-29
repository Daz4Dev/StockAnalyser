const express = require('express');
const axios = require('axios');
const bodyParser = require('body-parser');
const cors = require('cors');

const app = express();
const PORT = 3000;

app.use(cors());
app.use(bodyParser.json());

// Endpoint to receive data from the front-end
app.post('/analyze-stock', async (req, res) => {
    try {
        const { stock, indicators } = req.body;
      
        // Forward data to Python service
        const response = await axios.post('http://localhost:5000/compute-signals', {
            stock,
            indicators           
        },
        {
            timeout:60000
        });

        // Send the result back to the front-end
        res.json(response.data);


    } catch (error) {
        console.error('Error connecting to Python service:', error);
        res.status(500).json({ error: 'Error processing request' });
    }
});

app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
