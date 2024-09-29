$(document).ready(function() {
    // Fetch JSON data containing stocks and indicators
    fetch('List.json') // Ensure this path points to the correct location of your JSON file
        .then(response => response.json())
        .then(data => {
            // Populate stocks dropdown
            $('#stockSelector').select2({
                placeholder: 'Select a Stock',
                allowClear: true,
                data: data.stocks.map(stock => ({ id: stock, text: stock }))
            });

            // Dynamically generate the checklist for indicators
            const indicatorChecklist = document.getElementById('indicatorChecklist');
            for (let indicator in data.indicators) {
                const colDiv = document.createElement('div');
                colDiv.classList.add('col','mb-3');

                const cardDiv = document.createElement('div');
                cardDiv.classList.add('card', 'h-100', 'small-indicator-card');

                const cardBodyDiv = document.createElement('div');
                cardBodyDiv.classList.add('card-body', 'text-center');

                const checkbox = document.createElement('input');
                checkbox.type = 'checkbox';
                checkbox.id = indicator;
                checkbox.value = indicator;
                checkbox.classList.add('form-check-input');

                const label = document.createElement('label');
                label.htmlFor = indicator;
                label.textContent = indicator;
                label.classList.add('card-title', 'h6', 'd-block', 'mb-2');

                cardBodyDiv.appendChild(checkbox);
                cardBodyDiv.appendChild(label);

                cardDiv.appendChild(cardBodyDiv);
                colDiv.appendChild(cardDiv);
                indicatorChecklist.appendChild(colDiv);
            }
        })
        .catch(error => console.error('Error loading data:', error));

    // After user selects stock and indicators
    
    $('#compute-btn').click(function() {
        const selectedStock = $('#stockSelector').val();
        const selectedIndicators = [];

        // Gather selected indicators
        $('#indicatorChecklist input[type="checkbox"]').each(function() {
            if (this.checked) {
                selectedIndicators.push(this.value);
            }
        });

        //Send data to server
        fetch('http://localhost:3000/analyze-stock', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                stock: selectedStock,
                indicators: selectedIndicators
            }),
        })
        .then(response => response.json())
        .then(data => {
            console.log('Server responsed',data);
            
            const totalProfit = data.total_profit;
            const profitOutput = document.getElementById('profitOutput');
            const numTrades = data.num_trades;
            profitOutput.innerHTML = `
            <h5>Total Profit:</h5>
            <div class="profit-placeholder border border-dark p-3">
                <strong>${totalProfit ? totalProfit.toFixed(2) : 'N/A'}</strong> INR
            </div>
            <h5>Number of Trades in last year:</h5>
            <div class="trades-placeholder border border-dark p-3">
                <strong>${numTrades ? numTrades : 'N/A'}</strong>
            </div>
            `;

            // Display the plot
            const img = document.createElement('img');
            img.src = `data:image/png;base64,${data.plot}`;
            img.alt = 'Stock Analysis Plot';
            document.getElementById('plotContainer').appendChild(img);

        })
        .catch(error => console.error('Error:', error));
    });


});
