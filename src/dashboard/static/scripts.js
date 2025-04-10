// Global variables for charts
let topAffiliatesChart, signupsChart, volumeChart;

// Format numbers with commas
function formatNumber(num) {
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

// Format currency
function formatCurrency(num) {
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        minimumFractionDigits: 0,
        maximumFractionDigits: 0
    }).format(num);
}

// Format percentage
function formatPercentage(num) {
    return new Intl.NumberFormat('en-US', {
        style: 'percent',
        minimumFractionDigits: 1,
        maximumFractionDigits: 1
    }).format(num / 100);
}

// Format date
function formatDate(dateString) {
    return new Date(dateString).toLocaleString();
}

// Get status class
function getStatusClass(status) {
    switch (status) {
        case 'SUCCESS': return 'bg-success';
        case 'WARNING': return 'bg-warning';
        case 'ERROR': return 'bg-danger';
        default: return '';
    }
}

// Update ETL Status Table
async function updateETLStatus() {
    try {
        const response = await fetch('/api/etl-status');
        const data = await response.json();
        
        const tbody = document.querySelector('#etlStatusTable tbody');
        tbody.innerHTML = '';
        
        data.forEach(item => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>${item.data_source || '-'}</td>
                <td>${formatDate(item.last_load_time)}</td>
                <td class="${getStatusClass(item.etl_status)}">${item.etl_status}</td>
                <td>${formatNumber(item.total_records || 0)}</td>
                <td>${formatNumber(item.success_count || 0)}</td>
                <td>${formatNumber(item.error_count || 0)}</td>
                <td>${formatNumber(item.partial_count || 0)}</td>
            `;
            tbody.appendChild(row);
        });
    } catch (error) {
        console.error('Error updating ETL status:', error);
    }
}

// Update Top Affiliates Chart
async function updateTopAffiliatesChart() {
    try {
        const response = await fetch('/api/top-affiliates?limit=5');
        const data = await response.json();
        
        const labels = data.map(item => item.affiliate_name);
        const volumes = data.map(item => item.trading_volume_30d);
        
        if (topAffiliatesChart) {
            topAffiliatesChart.destroy();
        }
        
        const ctx = document.getElementById('topAffiliatesChart').getContext('2d');
        topAffiliatesChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Trading Volume (30d)',
                    data: volumes,
                    backgroundColor: 'rgba(54, 162, 235, 0.7)',
                    borderColor: 'rgba(54, 162, 235, 1)',
                    borderWidth: 2,
                    borderRadius: 8
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: true,
                        position: 'top'
                    },
                    tooltip: {
                        callbacks: {
                            label: (context) => {
                                return `Volume: ${formatCurrency(context.raw)}`;
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: value => formatCurrency(value)
                        },
                        grid: {
                            color: 'rgba(0, 0, 0, 0.1)'
                        }
                    },
                    x: {
                        grid: {
                            display: false
                        }
                    }
                }
            }
        });
    } catch (error) {
        console.error('Error updating top affiliates chart:', error);
    }
}

// Update Metrics Charts
async function updateMetricsCharts() {
    try {
        const response = await fetch('/api/affiliate-metrics');
        const data = await response.json();
        
        // New Signups Chart
        const signupsCtx = document.getElementById('newSignupsChart').getContext('2d');
        if (signupsChart) {
            signupsChart.destroy();
        }
        signupsChart = new Chart(signupsCtx, {
            type: 'line',
            data: {
                labels: data.map(d => d.affiliate_name),
                datasets: [{
                    label: 'New Signups (30d)',
                    data: data.map(d => d.new_signups_30d),
                    borderColor: 'rgba(75, 192, 192, 1)',
                    backgroundColor: 'rgba(75, 192, 192, 0.1)',
                    tension: 0.4,
                    fill: true,
                    pointRadius: 4,
                    pointHoverRadius: 6
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: true,
                        position: 'top'
                    },
                    tooltip: {
                        callbacks: {
                            label: (context) => {
                                return `Signups: ${formatNumber(context.raw)}`;
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: value => formatNumber(value)
                        },
                        grid: {
                            color: 'rgba(0, 0, 0, 0.1)'
                        }
                    },
                    x: {
                        grid: {
                            display: false
                        }
                    }
                }
            }
        });
        
        // Trading Volume Chart
        const volumeCtx = document.getElementById('tradingVolumeChart').getContext('2d');
        if (volumeChart) {
            volumeChart.destroy();
        }
        volumeChart = new Chart(volumeCtx, {
            type: 'line',
            data: {
                labels: data.map(d => d.affiliate_name),
                datasets: [{
                    label: 'Trading Volume (30d)',
                    data: data.map(d => d.trading_volume_30d),
                    borderColor: 'rgba(153, 102, 255, 1)',
                    backgroundColor: 'rgba(153, 102, 255, 0.1)',
                    tension: 0.4,
                    fill: true,
                    pointRadius: 4,
                    pointHoverRadius: 6
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: true,
                        position: 'top'
                    },
                    tooltip: {
                        callbacks: {
                            label: (context) => {
                                return `Volume: ${formatCurrency(context.raw)}`;
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: value => formatCurrency(value)
                        },
                        grid: {
                            color: 'rgba(0, 0, 0, 0.1)'
                        }
                    },
                    x: {
                        grid: {
                            display: false
                        }
                    }
                }
            }
        });
    } catch (error) {
        console.error('Error updating metrics charts:', error);
    }
}

// Update Metrics Table
async function updateMetricsTable() {
    try {
        const response = await fetch('/api/affiliate-metrics');
        const data = await response.json();
        
        const tbody = document.querySelector('#metricsTable tbody');
        tbody.innerHTML = '';
        
        data.forEach(item => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>${item.affiliate_name || '-'}</td>
                <td>${formatNumber(item.total_customers || 0)}</td>
                <td>${formatNumber(item.new_signups_30d || 0)}</td>
                <td>${formatNumber(item.active_customers_30d || 0)}</td>
                <td>${formatCurrency(item.trading_volume_30d || 0)}</td>
                <td>${formatPercentage(item.monthly_activation_rate || 0)}</td>
                <td>${formatCurrency(item.avg_trade_size || 0)}</td>
            `;
            tbody.appendChild(row);
        });
    } catch (error) {
        console.error('Error updating metrics table:', error);
    }
}

// Initialize dashboard
async function initializeDashboard() {
    try {
        console.log('Initializing dashboard...');
        
        // Update ETL Status
        console.log('Updating ETL status...');
        await updateETLStatus();
        
        // Update Top Affiliates Chart
        console.log('Updating top affiliates chart...');
        await updateTopAffiliatesChart();
        
        // Update Metrics Charts
        console.log('Updating metrics charts...');
        await updateMetricsCharts();
        
        // Update Metrics Table
        console.log('Updating metrics table...');
        await updateMetricsTable();
        
        console.log('Dashboard initialization complete');
        
        // Refresh data every minute
        setInterval(async () => {
            try {
                console.log('Refreshing dashboard data...');
                await updateETLStatus();
                await updateTopAffiliatesChart();
                await updateMetricsCharts();
                await updateMetricsTable();
                console.log('Dashboard refresh complete');
            } catch (error) {
                console.error('Error during dashboard refresh:', error);
            }
        }, 60000);
    } catch (error) {
        console.error('Error initializing dashboard:', error);
    }
}

// Start the dashboard when the page loads
console.log('Waiting for DOM to load...');
document.addEventListener('DOMContentLoaded', () => {
    console.log('DOM loaded, starting dashboard initialization');
    initializeDashboard().catch(error => {
        console.error('Failed to initialize dashboard:', error);
    });
}); 