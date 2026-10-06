output "public_ip" {
  value = oci_core_instance.this.public_ip
}

output "ssh" {
  value = "ssh ubuntu@${oci_core_instance.this.public_ip}"
}

output "tunnel" {
  description = "Then open http://localhost:3000 (Grafana), :9090 (Prometheus), :8080 (Redpanda Console)"
  value       = "ssh -N -L 3000:localhost:3000 -L 9090:localhost:9090 -L 8080:localhost:8080 ubuntu@${oci_core_instance.this.public_ip}"
}

output "follow_bootstrap" {
  value = "ssh ubuntu@${oci_core_instance.this.public_ip} 'tail -f /var/log/cloud-init-output.log'"
}
