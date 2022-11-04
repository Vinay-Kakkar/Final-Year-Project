

from Bio.PDB import *
parser = PDBParser(PERMISSIVE=True)



structure_id = 'id'
fileName = 'Proof of Concepts/PDB into rdd/pdb.pdb'
structure = parser.get_structure(structure_id, fileName)
atoms = []
for item in structure.get_atoms():
    atom = []
    atom.append(item.serial_number)
    atom.append(item.name)
    atom.append(item.get_parent().get_resname()) 
    (structure_id, model_id, chain_id, residue_id, atom_name,) = item.get_full_id()
    atom.append(chain_id)
    atom.append(residue_id) #residue_id[1] will return just the number
    atom.append(item.coord) #this also returns dtype=float32 for some reason
    atom.append(item.occupancy)
    atom.append(item.bfactor)
    atoms.append(atom)
print(atoms[0])
