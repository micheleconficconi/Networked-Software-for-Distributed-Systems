#include "contiki.h"
#include "net/routing/routing.h"
#include "net/netstack.h"
#include "net/ipv6/simple-udp.h"
#include "net/routing/rpl-lite/rpl.h"
#include <stdint.h>

#include "sys/log.h"
#define LOG_MODULE "App"
#define LOG_LEVEL LOG_LEVEL_INFO

#define UDP_CLIENT_PORT 8765
#define UDP_SERVER_PORT 5678
#define START_INTERVAL  (15 * CLOCK_SECOND)

enum {
  OP_LOCK = 1,
  OP_READ = 2,
  OP_WRITE = 3,
  OP_READ_REPLY = 4,
  OP_WRITE_ERROR = 5
};

typedef struct {
  uint8_t op_code;
  int value;
} msg_t;

static struct simple_udp_connection udp_conn;

PROCESS(udp_client_process, "UDP client 1");
AUTOSTART_PROCESSES(&udp_client_process);
static void
udp_rx_callback(struct simple_udp_connection *c,
         const uip_ipaddr_t *sender_addr,
         uint16_t sender_port,
         const uip_ipaddr_t *receiver_addr,
         uint16_t receiver_port,
         const uint8_t *data,
         uint16_t datalen)
{
  if(datalen != sizeof(msg_t)) {
    return;
  }

  static msg_t rx_msg;
  rx_msg = *(const msg_t *)data;

  if(rx_msg.op_code == OP_READ_REPLY) {
    LOG_INFO("Received response %d from ", rx_msg.value);
    LOG_INFO_6ADDR(sender_addr);
    LOG_INFO_("\n");
  } else if(rx_msg.op_code == OP_WRITE_ERROR) {
    LOG_INFO("Unable to perform the write operation\n");
  }
}
PROCESS_THREAD(udp_client_process, ev, data)
{
  static struct etimer periodic_timer;
  static uip_ipaddr_t dest_ipaddr;
  static msg_t tx_msg;

  PROCESS_BEGIN();

  simple_udp_register(&udp_conn, UDP_CLIENT_PORT, NULL,
                      UDP_SERVER_PORT, udp_rx_callback);

  etimer_set(&periodic_timer, START_INTERVAL);
  PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&periodic_timer));

  if(NETSTACK_ROUTING.node_is_reachable() && NETSTACK_ROUTING.get_root_ipaddr(&dest_ipaddr)) {
    LOG_INFO("Sending lock request to ");
    LOG_INFO_6ADDR(&dest_ipaddr);
    LOG_INFO_("\n");
    tx_msg.op_code = OP_LOCK;
    tx_msg.value = 0;
    simple_udp_sendto(&udp_conn, &tx_msg, sizeof(tx_msg), &dest_ipaddr);

    etimer_set(&periodic_timer, 2 * CLOCK_SECOND);
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&periodic_timer));

    LOG_INFO("Sending write(2) request to ");
    LOG_INFO_6ADDR(&dest_ipaddr);
    LOG_INFO_("\n");
    tx_msg.op_code = OP_WRITE;
    tx_msg.value = 2;
    simple_udp_sendto(&udp_conn, &tx_msg, sizeof(tx_msg), &dest_ipaddr);

    etimer_set(&periodic_timer, 10 * CLOCK_SECOND);
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&periodic_timer));

    LOG_INFO("Sending lock request to ");
    LOG_INFO_6ADDR(&dest_ipaddr);
    LOG_INFO_("\n");
    tx_msg.op_code = OP_LOCK;
    tx_msg.value = 0;
    simple_udp_sendto(&udp_conn, &tx_msg, sizeof(tx_msg), &dest_ipaddr);

    etimer_set(&periodic_timer, 2 * CLOCK_SECOND);
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&periodic_timer));

    LOG_INFO("Sending read request to ");
    LOG_INFO_6ADDR(&dest_ipaddr);
    LOG_INFO_("\n");
    tx_msg.op_code = OP_READ;
    tx_msg.value = 0;
    simple_udp_sendto(&udp_conn, &tx_msg, sizeof(tx_msg), &dest_ipaddr);

    etimer_set(&periodic_timer, 11 * CLOCK_SECOND);
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&periodic_timer));

    LOG_INFO("Sending write(4) request to ");
    LOG_INFO_6ADDR(&dest_ipaddr);
    LOG_INFO_("\n");
    tx_msg.op_code = OP_WRITE;
    tx_msg.value = 4;
    simple_udp_sendto(&udp_conn, &tx_msg, sizeof(tx_msg), &dest_ipaddr);

    etimer_set(&periodic_timer, 10 * CLOCK_SECOND);
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&periodic_timer));

    LOG_INFO("Sending read request to ");
    LOG_INFO_6ADDR(&dest_ipaddr);
    LOG_INFO_("\n");
    tx_msg.op_code = OP_READ;
    tx_msg.value = 0;
    simple_udp_sendto(&udp_conn, &tx_msg, sizeof(tx_msg), &dest_ipaddr);
  } else {
    LOG_INFO("Not reachable yet\n");
  }

  PROCESS_END();
}
/*---------------------------------------------------------------------------*/