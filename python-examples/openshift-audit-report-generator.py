#!/usr/bin/env python3
"""
OpenShift Cluster Audit and Report Generator
A comprehensive script to audit OpenShift clusters and generate detailed reports.
"""

import os
import json
import yaml
import subprocess
import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
import argparse
import logging


@dataclass
class AuditResult:
    """Data class to store audit results"""
    category: str
    check_name: str
    status: str  # PASS, WARN, FAIL
    message: str
    details: Optional[Dict] = None
    remediation: Optional[str] = None


class OpenShiftAuditor:
    def __init__(self, kubeconfig_path: Optional[str] = None, namespace: Optional[str] = None):
        self.kubeconfig_path = kubeconfig_path
        self.namespace = namespace
        self.results: List[AuditResult] = []
        self.cluster_info = {}
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Set kubeconfig if provided
        if kubeconfig_path:
            os.environ['KUBECONFIG'] = kubeconfig_path

    def run_oc_command(self, command: List[str]) -> Dict[str, Any]:
        """Execute oc command and return parsed JSON output"""
        try:
            cmd = ['oc'] + command
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                check=True,
                timeout=30
            )
            
            if result.stdout.strip():
                try:
                    return json.loads(result.stdout)
                except json.JSONDecodeError:
                    return {"raw_output": result.stdout}
            return {}
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Command failed: {' '.join(cmd)}")
            self.logger.error(f"Error: {e.stderr}")
            return {"error": e.stderr}
        except subprocess.TimeoutExpired:
            self.logger.error(f"Command timed out: {' '.join(cmd)}")
            return {"error": "Command timed out"}

    def get_cluster_info(self):
        """Gather basic cluster information"""
        self.logger.info("Gathering cluster information...")
        
        # Get cluster version
        version_info = self.run_oc_command(['get', 'clusterversion', '-o', 'json'])
        
        # Get nodes
        nodes_info = self.run_oc_command(['get', 'nodes', '-o', 'json'])
        
        # Get cluster operators
        operators_info = self.run_oc_command(['get', 'clusteroperators', '-o', 'json'])
        
        self.cluster_info = {
            'version': version_info,
            'nodes': nodes_info,
            'operators': operators_info,
            'audit_timestamp': datetime.datetime.now().isoformat()
        }

    def audit_cluster_health(self):
        """Audit cluster health status"""
        self.logger.info("Auditing cluster health...")
        
        # Check cluster operators
        operators = self.cluster_info.get('operators', {}).get('items', [])
        failed_operators = []
        
        for operator in operators:
            name = operator.get('metadata', {}).get('name', 'Unknown')
            conditions = operator.get('status', {}).get('conditions', [])
            
            available = False
            progressing = False
            degraded = False
            
            for condition in conditions:
                if condition.get('type') == 'Available' and condition.get('status') == 'True':
                    available = True
                elif condition.get('type') == 'Progressing' and condition.get('status') == 'True':
                    progressing = True
                elif condition.get('type') == 'Degraded' and condition.get('status') == 'True':
                    degraded = True
            
            if not available or degraded:
                failed_operators.append(name)
        
        if failed_operators:
            self.results.append(AuditResult(
                category="Cluster Health",
                check_name="Cluster Operators Status",
                status="FAIL",
                message=f"Failed operators: {', '.join(failed_operators)}",
                details={"failed_operators": failed_operators},
                remediation="Check operator logs and troubleshoot failed operators"
            ))
        else:
            self.results.append(AuditResult(
                category="Cluster Health",
                check_name="Cluster Operators Status",
                status="PASS",
                message="All cluster operators are healthy"
            ))

    def audit_node_health(self):
        """Audit node health and resources"""
        self.logger.info("Auditing node health...")
        
        nodes = self.cluster_info.get('nodes', {}).get('items', [])
        unhealthy_nodes = []
        
        for node in nodes:
            name = node.get('metadata', {}).get('name', 'Unknown')
            conditions = node.get('status', {}).get('conditions', [])
            
            ready = False
            for condition in conditions:
                if condition.get('type') == 'Ready' and condition.get('status') == 'True':
                    ready = True
                    break
            
            if not ready:
                unhealthy_nodes.append(name)
        
        if unhealthy_nodes:
            self.results.append(AuditResult(
                category="Node Health",
                check_name="Node Ready Status",
                status="FAIL",
                message=f"Unhealthy nodes: {', '.join(unhealthy_nodes)}",
                details={"unhealthy_nodes": unhealthy_nodes},
                remediation="Investigate node issues and ensure nodes are properly configured"
            ))
        else:
            self.results.append(AuditResult(
                category="Node Health",
                check_name="Node Ready Status",
                status="PASS",
                message="All nodes are ready"
            ))

    def audit_security_policies(self):
        """Audit security policies and configurations"""
        self.logger.info("Auditing security policies...")
        
        # Check for SecurityContextConstraints
        scc_info = self.run_oc_command(['get', 'scc', '-o', 'json'])
        
        if 'error' in scc_info:
            self.results.append(AuditResult(
                category="Security",
                check_name="SecurityContextConstraints",
                status="WARN",
                message="Unable to retrieve SecurityContextConstraints",
                remediation="Check RBAC permissions for SCC access"
            ))
        else:
            sccs = scc_info.get('items', [])
            privileged_sccs = []
            
            for scc in sccs:
                name = scc.get('metadata', {}).get('name', '')
                if scc.get('allowPrivilegedContainer', False):
                    privileged_sccs.append(name)
            
            if privileged_sccs:
                self.results.append(AuditResult(
                    category="Security",
                    check_name="Privileged SecurityContextConstraints",
                    status="WARN",
                    message=f"Found privileged SCCs: {', '.join(privileged_sccs)}",
                    details={"privileged_sccs": privileged_sccs},
                    remediation="Review and minimize use of privileged SCCs"
                ))
            else:
                self.results.append(AuditResult(
                    category="Security",
                    check_name="Privileged SecurityContextConstraints",
                    status="PASS",
                    message="No privileged SCCs found"
                ))

    def audit_resource_usage(self):
        """Audit resource usage and quotas"""
        self.logger.info("Auditing resource usage...")
        
        # Get resource quotas
        quotas_info = self.run_oc_command(['get', 'resourcequotas', '--all-namespaces', '-o', 'json'])
        
        if 'error' not in quotas_info:
            quotas = quotas_info.get('items', [])
            exceeded_quotas = []
            
            for quota in quotas:
                name = quota.get('metadata', {}).get('name', '')
                namespace = quota.get('metadata', {}).get('namespace', '')
                status = quota.get('status', {})
                hard = status.get('hard', {})
                used = status.get('used', {})
                
                for resource, limit in hard.items():
                    if resource in used:
                        # Simple check for exceeded quotas (this is a basic implementation)
                        if resource.endswith('.cpu') or resource.endswith('.memory'):
                            exceeded_quotas.append(f"{namespace}/{name}")
                            break
            
            if exceeded_quotas:
                self.results.append(AuditResult(
                    category="Resource Usage",
                    check_name="Resource Quotas",
                    status="WARN",
                    message=f"Quotas near limits: {', '.join(set(exceeded_quotas))}",
                    remediation="Review resource quotas and adjust as needed"
                ))
            else:
                self.results.append(AuditResult(
                    category="Resource Usage",
                    check_name="Resource Quotas",
                    status="PASS",
                    message="Resource quotas are within limits"
                ))

    def audit_pod_security(self):
        """Audit pod security configurations"""
        self.logger.info("Auditing pod security...")
        
        # Get pods with potential security issues
        pods_info = self.run_oc_command(['get', 'pods', '--all-namespaces', '-o', 'json'])
        
        if 'error' not in pods_info:
            pods = pods_info.get('items', [])
            privileged_pods = []
            root_pods = []
            
            for pod in pods:
                name = pod.get('metadata', {}).get('name', '')
                namespace = pod.get('metadata', {}).get('namespace', '')
                spec = pod.get('spec', {})
                
                for container in spec.get('containers', []):
                    security_context = container.get('securityContext', {})
                    
                    if security_context.get('privileged', False):
                        privileged_pods.append(f"{namespace}/{name}")
                    
                    if security_context.get('runAsUser') == 0:
                        root_pods.append(f"{namespace}/{name}")
            
            if privileged_pods:
                self.results.append(AuditResult(
                    category="Pod Security",
                    check_name="Privileged Pods",
                    status="WARN",
                    message=f"Found {len(privileged_pods)} privileged pods",
                    details={"privileged_pods": privileged_pods[:10]},  # Limit output
                    remediation="Review and minimize privileged pod usage"
                ))
            
            if root_pods:
                self.results.append(AuditResult(
                    category="Pod Security",
                    check_name="Root User Pods",
                    status="WARN",
                    message=f"Found {len(root_pods)} pods running as root",
                    details={"root_pods": root_pods[:10]},  # Limit output
                    remediation="Configure pods to run as non-root users"
                ))

    def audit_network_policies(self):
        """Audit network policies"""
        self.logger.info("Auditing network policies...")
        
        # Get network policies
        netpol_info = self.run_oc_command(['get', 'networkpolicies', '--all-namespaces', '-o', 'json'])
        
        if 'error' not in netpol_info:
            netpols = netpol_info.get('items', [])
            
            if not netpols:
                self.results.append(AuditResult(
                    category="Network Security",
                    check_name="Network Policies",
                    status="WARN",
                    message="No network policies found",
                    remediation="Implement network policies to control pod-to-pod communication"
                ))
            else:
                self.results.append(AuditResult(
                    category="Network Security",
                    check_name="Network Policies",
                    status="PASS",
                    message=f"Found {len(netpols)} network policies"
                ))

    def run_audit(self):
        """Run complete audit"""
        self.logger.info("Starting OpenShift cluster audit...")
        
        try:
            # Gather cluster information
            self.get_cluster_info()
            
            # Run audit checks
            self.audit_cluster_health()
            self.audit_node_health()
            self.audit_security_policies()
            self.audit_resource_usage()
            self.audit_pod_security()
            self.audit_network_policies()
            
            self.logger.info("Audit completed successfully")
            
        except Exception as e:
            self.logger.error(f"Audit failed: {str(e)}")
            self.results.append(AuditResult(
                category="System",
                check_name="Audit Execution",
                status="FAIL",
                message=f"Audit execution failed: {str(e)}"
            ))

    def generate_report(self, output_format: str = 'json', output_file: Optional[str] = None):
        """Generate audit report"""
        
        # Calculate summary statistics
        total_checks = len(self.results)
        passed = len([r for r in self.results if r.status == 'PASS'])
        warnings = len([r for r in self.results if r.status == 'WARN'])
        failed = len([r for r in self.results if r.status == 'FAIL'])
        
        report_data = {
            'audit_summary': {
                'timestamp': datetime.datetime.now().isoformat(),
                'total_checks': total_checks,
                'passed': passed,
                'warnings': warnings,
                'failed': failed,
                'cluster_info': {
                    'version': self.cluster_info.get('version', {}).get('status', {}).get('desired', {}).get('version', 'Unknown'),
                    'node_count': len(self.cluster_info.get('nodes', {}).get('items', []))
                }
            },
            'audit_results': [asdict(result) for result in self.results]
        }
        
        if output_format.lower() == 'json':
            report_content = json.dumps(report_data, indent=2)
        elif output_format.lower() == 'yaml':
            report_content = yaml.dump(report_data, default_flow_style=False)
        else:  # text format
            report_content = self._generate_text_report(report_data)
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(report_content)
            self.logger.info(f"Report saved to {output_file}")
        else:
            print(report_content)

    def _generate_text_report(self, report_data: Dict) -> str:
        """Generate human-readable text report"""
        summary = report_data['audit_summary']
        results = report_data['audit_results']
        
        report = f"""
OpenShift Cluster Audit Report
==============================
Generated: {summary['timestamp']}
Cluster Version: {summary['cluster_info']['version']}
Node Count: {summary['cluster_info']['node_count']}

AUDIT SUMMARY
=============
Total Checks: {summary['total_checks']}
Passed: {summary['passed']}
Warnings: {summary['warnings']}
Failed: {summary['failed']}

DETAILED RESULTS
================
"""
        
        # Group results by category
        categories = {}
        for result in results:
            category = result['category']
            if category not in categories:
                categories[category] = []
            categories[category].append(result)
        
        for category, category_results in categories.items():
            report += f"\n{category.upper()}\n"
            report += "=" * len(category) + "\n"
            
            for result in category_results:
                status_symbol = "✓" if result['status'] == 'PASS' else "⚠" if result['status'] == 'WARN' else "✗"
                report += f"{status_symbol} [{result['status']}] {result['check_name']}: {result['message']}\n"
                
                if result.get('remediation'):
                    report += f"  Remediation: {result['remediation']}\n"
                report += "\n"
        
        return report


def main():
    parser = argparse.ArgumentParser(description='OpenShift Cluster Audit and Report Generator')
    parser.add_argument('--kubeconfig', help='Path to kubeconfig file')
    parser.add_argument('--namespace', help='Specific namespace to focus on')
    parser.add_argument('--output-format', choices=['json', 'yaml', 'text'], default='text',
                       help='Output format for the report')
    parser.add_argument('--output-file', help='Output file path')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create auditor instance
    auditor = OpenShiftAuditor(
        kubeconfig_path=args.kubeconfig,
        namespace=args.namespace
    )
    
    # Run audit
    auditor.run_audit()
    
    # Generate report
    auditor.generate_report(
        output_format=args.output_format,
        output_file=args.output_file
    )


if __name__ == '__main__':
    main()