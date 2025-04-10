// Utility function to format dates
function formatDate(dateString) {
    return new Date(dateString).toLocaleString();
}

// Function to set status cell color
function setStatusColor(cell, status) {
    cell.classList.remove('bg-success', 'bg-warning', 'bg-danger');
    switch(status.toUpperCase()) {
        case 'SUCCESS':
            cell.classList.add('bg-success');
            break;
        case 'PARTIAL':
            cell.classList.add('bg-warning');
            break;
        case 'ERROR':
            cell.classList.add('bg-danger');
            break;
    }
}

// Function to update ETL status table
async function updateETLStatus() {
    try {
        const response = await fetch('/api/etl-status');
        const data = await response.json();
        
        const tbody = document.querySelector('#etl-status-table tbody');
        tbody.innerHTML = '';
        
        data.forEach(row => {
            const tr = document.createElement('tr');
            
            // Data source column
            tr.innerHTML = `
                <td>${row.table_name}</td>
                <td>${formatDate(row.last_load_time)}</td>
                <td class="status-cell">${row.etl_status}</td>
                <td>${row.total_records}</td>
                <td>${row.success_count}</td>
                <td>${row.error_count}</td>
                <td>${row.partial_count}</td>
                <td class="error-message">${row.error_messages || '-'}</td>
            `;
            
            // Set status color
            const statusCell = tr.querySelector('.status-cell');
            setStatusColor(statusCell, row.etl_status);
            
            tbody.appendChild(tr);
        });
    } catch (error) {
        console.error('Error updating ETL status:', error);
    }
}

// Function to create top affiliates chart
async function createTopAffiliatesChart() {
    try {
        const response = await fetch('/api/top-affiliates');
        const data = await response.json();
        
        const ctx = document.getElementById('topAffiliatesChart').getContext('2d');
        new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.map(d => d.affiliate_name),
                datasets: [{
                    label: 'Trading Volume',
                    data: data.map(d => d.monthly_trading_volume),
                    backgroundColor: 'rgba(54, 162, 235, 0.5)',
                    borderColor: 'rgba(54, 162, 235, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Trading Volume'
                        }
                    }
                }
            }
        });
    } catch (error) {
        console.error('Error creating top affiliates chart:', error);
    }
}

// Function to create affiliate metrics charts
async function createAffiliateMetricsCharts() {
    try {
        const response = await fetch('/api/affiliate-metrics');
        const data = await response.json();
        
        // New Signups Chart
        const signupsCtx = document.getElementById('newSignupsChart').getContext('2d');
        new Chart(signupsCtx, {
            type: 'line',
            data: {
                labels: data.map(d => d.affiliate_name),
                datasets: [{
                    label: 'New Signups',
                    data: data.map(d => d.new_signups),
                    borderColor: 'rgba(75, 192, 192, 1)',
                    tension: 0.1,
                    fill: false
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false
            }
        });
        
        // Trading Volume Chart
        const volumeCtx = document.getElementById('tradingVolumeChart').getContext('2d');
        new Chart(volumeCtx, {
            type: 'line',
            data: {
                labels: data.map(d => d.affiliate_name),
                datasets: [{
                    label: 'Trading Volume',
                    data: data.map(d => d.monthly_trading_volume),
                    borderColor: 'rgba(153, 102, 255, 1)',
                    tension: 0.1,
                    fill: false
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false
            }
        });
        
        // Update metrics table
        const tbody = document.querySelector('#affiliate-metrics-table tbody');
        tbody.innerHTML = '';
        
        data.forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${row.affiliate_name}</td>
                <td>${row.total_customers}</td>
                <td>${row.new_signups}</td>
                <td>${row.active_customers}</td>
                <td>${row.monthly_trading_volume.toLocaleString()}</td>
                <td>${(row.activation_rate * 100).toFixed(1)}%</td>
                <td>${row.avg_trade_size.toLocaleString()}</td>
            `;
            tbody.appendChild(tr);
        });
    } catch (error) {
        console.error('Error creating affiliate metrics charts:', error);
    }
}

// Initialize dashboard
async function initializeDashboard() {
    await updateETLStatus();
    await createTopAffiliatesChart();
    await createAffiliateMetricsCharts();
    
    // Refresh ETL status every minute
    setInterval(updateETLStatus, 60000);
}

// Start the dashboard when the page loads
document.addEventListener('DOMContentLoaded', initializeDashboard); 