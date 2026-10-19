/*
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the Institute nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE INSTITUTE AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE INSTITUTE OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * This file is part of the Contiki operating system.
 *
 */
#include "contiki.h"
#include "net/routing/routing.h"
#include "net/netstack.h"
#include "net/ipv6/simple-udp.h"
#include "net/routing/rpl-lite/rpl.h"
#include <stdbool.h>
#include <stdint.h>

#include "sys/log.h"
#define LOG_MODULE "App"
#define LOG_LEVEL LOG_LEVEL_INFO

#define UDP_CLIENT_PORT 8765
#define UDP_SERVER_PORT 5678
#define CTIMER_INTERVAL (5 * CLOCK_SECOND)

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
static int data_structure = 1;
static bool lock = false;
static uip_ipaddr_t lock_ip;
static struct ctimer lock_ctimer;

PROCESS(udp_server_process, "UDP server");
AUTOSTART_PROCESSES(&udp_server_process);
static void
ctimer_callback(void *data)
{
  lock = false;
  LOG_INFO("Timer expired, unlocked\n");
}
static void
udp_rx_callback(struct simple_udp_connection *c,
         const uip_ipaddr_t *sender_addr,
         uint16_t sender_port,
         const uip_ipaddr_t *receiver_addr,
         uint16_t receiver_port,
         const uint8_t *data,
         uint16_t datalen)
{
  /* Ensure the received packet matches our fixed structure size */
  if(datalen != sizeof(msg_t)) {
    LOG_WARN("Received packet with invalid size (%u bytes)\n", datalen);
    return;
  }

  static msg_t rx_msg;
  static msg_t tx_msg;
  rx_msg = *(const msg_t *)data;

  switch(rx_msg.op_code) {
    case OP_LOCK:
      LOG_INFO("Received LOCK request from ");
      LOG_INFO_6ADDR(sender_addr);
      LOG_INFO_("\n");

      if(!lock) {
        lock = true;
        uip_ipaddr_copy(&lock_ip, sender_addr);
        LOG_INFO("Locked by ");
        LOG_INFO_6ADDR(&lock_ip);
        LOG_INFO_("\n");
        ctimer_set(&lock_ctimer, CTIMER_INTERVAL, ctimer_callback, NULL);
        LOG_INFO("Timer started\n");
      } else {
        LOG_INFO("Already locked by ");
        LOG_INFO_6ADDR(&lock_ip);
        LOG_INFO_("\n");
      }
      break;

    case OP_READ:
      LOG_INFO("Received READ request, sending value %d to ", data_structure);
      LOG_INFO_6ADDR(sender_addr);
      LOG_INFO_("\n");

      tx_msg.op_code = OP_READ_REPLY;
      tx_msg.value = data_structure;
      simple_udp_sendto(&udp_conn, &tx_msg, sizeof(tx_msg), sender_addr);
      break;

    case OP_WRITE:
      LOG_INFO("Received WRITE(%d) request from ", rx_msg.value);
      LOG_INFO_6ADDR(sender_addr);
      LOG_INFO_("\n");

      if(lock && uip_ipaddr_cmp(&lock_ip, sender_addr)) {
        data_structure = rx_msg.value;
        LOG_INFO("Wrote %d to the data structure\n", data_structure);
        lock = false;
        ctimer_stop(&lock_ctimer);
      } else {
        LOG_INFO("Unable to perform write operation, sending error code to ");
        LOG_INFO_6ADDR(sender_addr);
        LOG_INFO_("\n");

        tx_msg.op_code = OP_WRITE_ERROR;
        tx_msg.value = 0;
        simple_udp_sendto(&udp_conn, &tx_msg, sizeof(tx_msg), sender_addr);
      }
      break;

    default:
      LOG_WARN("Unknown op_code: %u\n", rx_msg.op_code);
      break;
  }
}
PROCESS_THREAD(udp_server_process, ev, data)
{
  PROCESS_BEGIN();

  NETSTACK_ROUTING.root_start();

  simple_udp_register(&udp_conn, UDP_SERVER_PORT, NULL,
                      UDP_CLIENT_PORT, udp_rx_callback);

  PROCESS_END();
}
/*---------------------------------------------------------------------------*/