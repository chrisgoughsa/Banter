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
        case 'SUCCESS': return 'status-success';
        case 'WARNING': return 'status-warning';
        case 'ERROR': return 'status-error';
        default: return '';
    }
}

// Update ETL Status Table
async function updateETLStatus() {
    const response = await fetch('/api/etl-status');
    const data = await response.json();
    
    const tbody = document.querySelector('#etlStatusTable tbody');
    tbody.innerHTML = '';
    
    data.forEach(item => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${item.data_source}</td>
            <td>${formatDate(item.last_load_time)}</td>
            <td class="${getStatusClass(item.etl_status)}">${item.etl_status}</td>
            <td>${formatNumber(item.total_records)}</td>
            <td>${formatNumber(item.success_count)}</td>
            <td>${formatNumber(item.error_count)}</td>
            <td>${formatNumber(item.partial_count)}</td>
        `;
        tbody.appendChild(row);
    });
}

// Update Top Affiliates Chart
async function updateTopAffiliatesChart() {
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
}

// Update Metrics Charts
async function updateMetricsCharts() {
    const response = await fetch('/api/affiliate-metrics');
    const data = await response.json();
    
    // New Signups Chart
    const signupsCtx = document.getElementById('newSignupsChart').getContext('2d');
    new Chart(signupsCtx, {
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
    new Chart(volumeCtx, {
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
}

// Update Metrics Table
async function updateMetricsTable() {
    const response = await fetch('/api/affiliate-metrics');
    const data = await response.json();
    
    const tbody = document.querySelector('#metricsTable tbody');
    tbody.innerHTML = '';
    
    data.forEach(item => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${item.affiliate_name}</td>
            <td>${formatNumber(item.total_customers)}</td>
            <td>${formatNumber(item.new_signups_30d)}</td>
            <td>${formatNumber(item.active_customers_30d)}</td>
            <td>${formatCurrency(item.trading_volume_30d)}</td>
            <td>${formatPercentage(item.monthly_activation_rate)}</td>
            <td>${formatCurrency(item.avg_trade_size)}</td>
        `;
        tbody.appendChild(row);
    });
}

// Initialize dashboard
async function initializeDashboard() {
    await updateETLStatus();
    await updateTopAffiliatesChart();
    await updateMetricsCharts();
    await updateMetricsTable();
    
    // Refresh data every 5 minutes
    setInterval(async () => {
        await updateETLStatus();
        await updateTopAffiliatesChart();
        await updateMetricsCharts();
        await updateMetricsTable();
    }, 300000);
}

// Start the dashboard when the page loads
document.addEventListener('DOMContentLoaded', initializeDashboard); 