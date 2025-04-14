using System;
using Microsoft.Data.SqlClient;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using System.Threading.Tasks;
using System.Data;

namespace TicketApp
{
    // Model class
    public class TicketPurchase
    {
        public int ConcertId { get; set; }
        public string Email { get; set; }
        public string Name { get; set; }
        public string Phone { get; set; }
        public int Quantity { get; set; }
        public string CreditCard { get; set; }
        public string Expiration { get; set; }
        public string SecurityCode { get; set; }
        public string Address { get; set; }
        public string City { get; set; }
        public string Province { get; set; }
        public string PostalCode { get; set; }
        public string Country { get; set; }
        // PurchaseDate will be generated on insert
    }

    public class ProcessTicketPurchase
    {
        private readonly ILogger _logger;

        public ProcessTicketPurchase(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<ProcessTicketPurchase>();
        }

        [Function("ProcessTicketPurchase")]
        public async Task Run(
            [QueueTrigger("tickethub", Connection = "AzureWebJobsStorage")] string queueItem,
            FunctionContext context)
        {
            _logger.LogInformation($"Processing ticket purchase message: {queueItem}");

            TicketPurchase ticketPurchase = null;
            try
            {
                ticketPurchase = JsonSerializer.Deserialize<TicketPurchase>(queueItem);

                if (ticketPurchase == null)
                {
                    _logger.LogError("Failed to deserialize queue message or message was empty/null.");
                    return;
                }

                await InsertTicketPurchase(ticketPurchase);

                _logger.LogInformation($"Successfully processed ticket purchase for {ticketPurchase.Name}");
            }
            catch (JsonException jsonEx)
            {
                _logger.LogError(jsonEx, $"Error deserializing ticket purchase JSON. Message content: {queueItem}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error processing ticket purchase for {ticketPurchase?.Name}: {ex.Message}");
                throw; // Rethrow to ensure the message is not removed from the queue on error
            }
        }

        private async Task InsertTicketPurchase(TicketPurchase purchase)
        {
            string connectionString = Environment.GetEnvironmentVariable("SqlConnectionString");
            if (string.IsNullOrEmpty(connectionString))
            {
                _logger.LogError("SQL Connection String 'SqlConnectionString' is missing or empty in configuration.");
                throw new InvalidOperationException("Database connection string is not configured.");
            }

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                _logger.LogInformation("Database connection opened successfully.");

                string insertSql = @"
                INSERT INTO dbo.TicketPurchases
                (ConcertId, Email, Name, Phone, Quantity, CreditCard, Expiration,
                SecurityCode, Address, City, Province, PostalCode, Country, PurchaseDate)
                VALUES
                (@ConcertId, @Email, @Name, @Phone, @Quantity, @CreditCard, @Expiration,
                @SecurityCode, @Address, @City, @Province, @PostalCode, @Country, @PurchaseDate)";

                using SqlCommand command = new(insertSql, connection);
                command.Parameters.AddWithValue("@ConcertId", purchase.ConcertId);
                command.Parameters.AddWithValue("@Email", purchase.Email);
                command.Parameters.AddWithValue("@Name", purchase.Name);
                command.Parameters.AddWithValue("@Phone", purchase.Phone);
                command.Parameters.AddWithValue("@Quantity", purchase.Quantity);
                command.Parameters.AddWithValue("@CreditCard", purchase.CreditCard);
                command.Parameters.AddWithValue("@Expiration", purchase.Expiration);
                command.Parameters.AddWithValue("@SecurityCode", purchase.SecurityCode);
                command.Parameters.AddWithValue("@Address", purchase.Address);
                command.Parameters.AddWithValue("@City", purchase.City);
                command.Parameters.AddWithValue("@Province", purchase.Province);
                command.Parameters.AddWithValue("@PostalCode", purchase.PostalCode);
                command.Parameters.AddWithValue("@Country", purchase.Country);
                // Generate PurchaseDate on insert
                command.Parameters.AddWithValue("@PurchaseDate", DateTime.UtcNow);

                await command.ExecuteNonQueryAsync();
                _logger.LogInformation("Database record inserted successfully");
            }
        }
    }
}